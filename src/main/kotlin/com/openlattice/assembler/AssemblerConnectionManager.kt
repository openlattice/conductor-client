/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.assembler

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.MetricRegistry.name
import com.codahale.metrics.Timer
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.openlattice.assembler.PostgresDatabases.Companion.buildOrganizationDatabaseName
import com.openlattice.assembler.PostgresRoles.Companion.buildOrganizationRoleName
import com.openlattice.assembler.PostgresRoles.Companion.buildOrganizationUserId
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresRoleName
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresUsername
import com.openlattice.authorization.*
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.*
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.openlattice.principals.RoleCreatedEvent
import com.openlattice.principals.UserCreatedEvent
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Connection
import java.sql.Statement
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Function
import java.util.function.Supplier
import kotlin.NoSuchElementException

/**
 * Handles all direct database operations for Assemblies including
 * - creating roles and permissions
 * - assembling data into external tables
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class AssemblerConnectionManager(
        private val assemblerConfiguration: AssemblerConfiguration,
        private val hds: HikariDataSource,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val organizations: HazelcastOrganizationService,
        private val dbCredentialService: DbCredentialService,
        eventBus: EventBus,
        metricRegistry: MetricRegistry
) {

    private val perDbCache: LoadingCache<String, HikariDataSource> = CacheBuilder
            .newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(cacheLoader())

    /**
     * Default database object in the target database server
     */
    private val target: HikariDataSource = connect("postgres")
    private val materializeAllTimer: Timer =
            metricRegistry.timer(name(AssemblerConnectionManager::class.java, "materializeAll"))
    private val materializeEntitySetsTimer: Timer =
            metricRegistry.timer(name(AssemblerConnectionManager::class.java, "materializeEntitySets"))

    private val targetUsername: String? = assemblerConfiguration.server["username"] as String?

    init {
        if ( targetUsername == null || targetUsername.isBlank() ) {
            throw Exception("No 'username' property specified in assembler configuration server block")
        }
        eventBus.register(this)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AssemblerConnectionManager::class.java)

        const val INTEGRATIONS_SCHEMA = "integrations"
        const val PUBLIC_ROLE = "public"

        @JvmStatic
        val MATERIALIZED_VIEWS_SCHEMA = "openlattice"
        @JvmStatic
        val PUBLIC_SCHEMA = "public"

        @JvmStatic
        fun entitySetNameTableName(entitySetName: String): String {
            return "$MATERIALIZED_VIEWS_SCHEMA.${quote(entitySetName)}"
        }

        @JvmStatic
        fun createDataSource(dbName: String, config: Properties, useSsl: Boolean): HikariDataSource {
            config.computeIfPresent("jdbcUrl") { _, jdbcUrl ->
                "${(jdbcUrl as String).removeSuffix(
                        "/"
                )}/$dbName" + if (useSsl) {
                    "?sslmode=require"
                } else {
                    ""
                }
            }
            return HikariDataSource(HikariConfig(config))
        }
    }

    fun cacheLoader(): CacheLoader<String, HikariDataSource> {
        return CacheLoader.from { dbName ->
            createDataSource(dbName!!, assemblerConfiguration.server.clone() as Properties, assemblerConfiguration.ssl)
        }
    }

    fun connect(dbName: String): HikariDataSource {
        return perDbCache.get(dbName)
    }

    @Subscribe
    fun handleUserCreated(userCreatedEvent: UserCreatedEvent) {
        createUnprivilegedUser(userCreatedEvent.user)
    }

    @Subscribe
    fun handleRoleCreated(roleCreatedEvent: RoleCreatedEvent) {
        createRole(roleCreatedEvent.role)
    }

    /**
     * Creates a private organization database using information stored under [organizationId]
     * that can be used for uploading data using launchpad.
     */
    fun createOrganizationDatabase(organizationId: UUID) {
        logger.info("Creating organization database for organization with id $organizationId")
        val organization = organizations.getOrganization(organizationId)
        val dbName = buildOrganizationDatabaseName(organizationId)
        createOrganizationDatabase(organizationId, dbName)

        connect(dbName).let { dataSource ->
            revokeAllPublicSchemaAccess(dataSource)
            createSchema(dataSource, MATERIALIZED_VIEWS_SCHEMA)
            createSchema(dataSource, INTEGRATIONS_SCHEMA)
            configureOrganizationUser(organizationId, dataSource)
            addMembersToOrganization(dbName, dataSource, organization.members)
            configureServerUser(dataSource)
        }
    }

    /**
     * Create schema [schemaName] in [dataSource] if it does not already exist
     */
    private fun createSchema(dataSource: HikariDataSource, schemaName: String) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE SCHEMA IF NOT EXISTS $schemaName")
            }
        }
    }

    /**
     * Sets search_path to all of [INTEGRATIONS_SCHEMA],[MATERIALIZED_VIEWS_SCHEMA],[PUBLIC_SCHEMA] in [dataSource]
     * for role specified by 'username' property in [assemblerConfiguration.server]
     */
    private fun configureServerUser(dataSource: HikariDataSource) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                        "ALTER ROLE $targetUsername SET search_path to " +
                                "$INTEGRATIONS_SCHEMA,$MATERIALIZED_VIEWS_SCHEMA,$PUBLIC_SCHEMA"
                )
            }
        }
    }

    /**
     * Grants USAGE and CREATE to [organizationId]'s organization user on [MATERIALIZED_VIEWS_SCHEMA]
     *  and sets the search_path for the organization user to [MATERIALIZED_VIEWS_SCHEMA] in [dataSource]
     */
    private fun configureOrganizationUser(organizationId: UUID, dataSource: HikariDataSource) {
        val dbOrgUser = quote(buildOrganizationUserId(organizationId))
        dataSource.connection.createStatement().use { statement ->
            //Allow usage and create on schema openlattice to organization user
            statement.execute("GRANT USAGE, CREATE ON SCHEMA $MATERIALIZED_VIEWS_SCHEMA TO $dbOrgUser")
            statement.execute("ALTER USER $dbOrgUser SET search_path TO $MATERIALIZED_VIEWS_SCHEMA")
        }
    }

    /**
     * Filters invalid principals out of [members], then calls [configureUsersInDatabase] on [dataSource]
     */
    private fun addMembersToOrganization(dbName: String, dataSource: HikariDataSource, members: Set<Principal>) {
        logger.info("Configuring members for organization database {}", dbName)
        val validUserPrincipals = members.filter {
            val isSystemRole = (it.id == SystemRole.OPENLATTICE.principal.id && it.id == SystemRole.ADMIN.principal.id)
            if (isSystemRole) {
                return@filter false
            }
            val principalExists = securePrincipalsManager.principalExists(it)
            if (!principalExists) {
                logger.warn("Principal {} does not exists", it)
            }
            return@filter principalExists
        } //There are some bad principals in the member list some how-- probably from testing.

        val securablePrincipalsToAdd = securePrincipalsManager.getSecurablePrincipals(validUserPrincipals)
        if (securablePrincipalsToAdd.isNotEmpty()) {
            configureUsersInDatabase(dataSource, dbName, securablePrincipalsToAdd)
        }
    }

    internal fun addMembersToOrganization(
            dbName: String,
            dataSource: HikariDataSource,
            authorizedPropertyTypesOfEntitySetsByPrincipal: Map<SecurablePrincipal, Map<EntitySet, Collection<PropertyType>>>
    ) {
        if (authorizedPropertyTypesOfEntitySetsByPrincipal.isNotEmpty()) {

            val authorizedPropertyTypesOfEntitySetsByPostgresUser = authorizedPropertyTypesOfEntitySetsByPrincipal
                    .mapKeys { quote(buildPostgresUsername(it.key)) }
            val userNames = authorizedPropertyTypesOfEntitySetsByPrincipal.keys
            configureUsersInDatabase(dataSource, dbName, userNames)
            dataSource.connection.use { connection ->
                grantSelectForNewMembers(connection, authorizedPropertyTypesOfEntitySetsByPostgresUser)
            }
        }
    }

    fun removeMembersFromOrganization(
            dbName: String,
            dataSource: HikariDataSource,
            principals: Collection<SecurablePrincipal>
    ) {
        if (principals.isNotEmpty()) {
            val userNames = principals.map { quote(buildPostgresUsername(it)) }
            revokeConnectAndSchemaUsage(dataSource, dbName, userNames)
        }
    }

    /**
     * Get/create admin creds for [organizationId] org user
     * Create org role for [organizationId]
     * Create org user for [organizationId]
     * Grant org role to org user for [organizationId]
     * Create org db for [organizationId]
     * Grant [MEMBER_ORG_DATABASE_PERMISSIONS] on [dbName] to org user
     * Revoke all [PUBLIC_ROLE] access on [dbName]
     */
    private fun createOrganizationDatabase(organizationId: UUID, dbName: String) {
        val db = quote(dbName)
        val dbRole = buildOrganizationRoleName(dbName)
        val unquotedDbAdminUser = buildOrganizationUserId(organizationId)
        val dbOrgUser = quote(unquotedDbAdminUser)
        val dbAdminUserPassword = dbCredentialService.getOrCreateUserCredentials(unquotedDbAdminUser)
                ?: dbCredentialService.getDbCredential(unquotedDbAdminUser)
        val createOrgDbRoleSql = createRoleIfNotExistsSql(dbRole)
        val createOrgDbUserSql = createUserIfNotExistsSql(unquotedDbAdminUser, dbAdminUserPassword)

        val grantRole = "GRANT ${quote(dbRole)} TO $dbOrgUser"
        val createDb = "CREATE DATABASE $db"
        val revokeAll = "REVOKE ALL ON DATABASE $db FROM $PUBLIC_ROLE"

        //We connect to default db in order to do initial db setup

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(createOrgDbRoleSql)
                statement.execute(createOrgDbUserSql)
                statement.execute(grantRole)
                if (!exists(dbName)) {
                    statement.execute(createDb)
                    statement.execute(
                            "GRANT ${MEMBER_ORG_DATABASE_PERMISSIONS.joinToString(", ")} " +
                                    "ON DATABASE $db TO $dbOrgUser"
                    )
                }
                statement.execute(revokeAll)
                return@use
            }
        }
    }

    internal fun dropOrganizationDatabase(organizationId: UUID) {
        val dbName = buildOrganizationDatabaseName(organizationId)
        val db = quote(dbName)
        val dbRole = quote(buildOrganizationRoleName(dbName))
        val unquotedDbAdminUser = buildOrganizationUserId(organizationId)
        val dbAdminUser = quote(unquotedDbAdminUser)

        val dropDb = " DROP DATABASE $db"
        val dropDbUser = "DROP ROLE $dbAdminUser"
        //TODO: If we grant this role to other users, we need to make sure we drop it
        val dropDbRole = "DROP ROLE $dbRole"


        //We connect to default db in order to do initial db setup

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(dropDb)
                statement.execute(dropDbUser)
                statement.execute(dropDbRole)
                return@use
            }
        }
    }

    /**
     * Connect to [organizationId] org database, materialize [authorizedPropertyTypesByEntitySet] property types
     * and grant select to [authorizedPropertyTypesOfPrincipalsByEntitySetId] principals on property type columns
     */
    internal fun materializeEntitySets(
            organizationId: UUID,
            authorizedPropertyTypesByEntitySet: Map<EntitySet, Map<UUID, PropertyType>>,
            authorizedPropertyTypesOfPrincipalsByEntitySetId: Map<UUID, Map<Principal, Set<PropertyType>>>
    ): Map<UUID, Set<OrganizationEntitySetFlag>> {
        logger.info(
                "Materializing entity sets ${authorizedPropertyTypesByEntitySet.keys.map { it.id }} in " +
                        "organization $organizationId database."
        )

        materializeAllTimer.time().use {
            connect(buildOrganizationDatabaseName(organizationId)).let { dataSource ->
                return materializeEntitySets(
                        dataSource,
                        authorizedPropertyTypesByEntitySet,
                        authorizedPropertyTypesOfPrincipalsByEntitySetId
                )
            }
        }
    }

    /**
     * Assemble [authorizedPropertyTypesByEntitySet] property types and grant select
     * to [authorizedPropertyTypesOfPrincipalsByEntitySetId] principals on property type columns using [dataSource]
     */
    private fun materializeEntitySets(
            dataSource: HikariDataSource,
            authorizedPropertyTypesByEntitySet: Map<EntitySet, Map<UUID, PropertyType>>,
            authorizedPropertyTypesOfPrincipalsByEntitySetId: Map<UUID, Map<Principal, Set<PropertyType>>>
    ): Map<UUID, Set<OrganizationEntitySetFlag>> {
        return authorizedPropertyTypesByEntitySet.map { (entitySet, assemblablePropertyTypesById) ->
            materialize(
                    dataSource,
                    entitySet,
                    assemblablePropertyTypesById,
                    authorizedPropertyTypesOfPrincipalsByEntitySetId.getValue(entitySet.id)
            )
            entitySet.id to EnumSet.of(OrganizationEntitySetFlag.MATERIALIZED)
        }.toMap()
    }

    /**
     * TODO - currently broken :'(
     * Assemble [entitySet] as a table named for [entitySet] with columns named by [assemblablePropertyTypesById]
     * and permissions applied by [authorizedPropertyTypesOfPrincipals]
     */
    private fun materialize(
            dataSource: HikariDataSource,
            entitySet: EntitySet,
            assemblablePropertyTypesById: Map<UUID, PropertyType>,
            authorizedPropertyTypesOfPrincipals: Map<Principal, Set<PropertyType>>
    ) {
        materializeEntitySetsTimer.time().use {
            val tableName = entitySetNameTableName(entitySet.name)

            dataSource.connection.let { connection ->
                // first drop and create materialized view
                // TODO ^this is not happening^
                logger.info("Materialized entity set ${entitySet.id}")

                //Next we need to grant select on materialize view to everyone who has permission.
                val selectGrantedResults = grantSelectForEntitySet(
                        connection,
                        tableName,
                        authorizedPropertyTypesOfPrincipals
                )
                logger.info(
                        "Granted select for ${selectGrantedResults.filter { it >= 0 }.size} users/roles " +
                                "on materialized view $tableName"
                )
            }
        }
    }

    private val SELECT_COLS_FOR_ASSEMBLY = listOf(ENTITY_SET_ID.name, ID_VALUE.name, ENTITY_KEY_IDS_COL.name)

    private fun getSelectColumnsForMaterializedView(propertyTypes: Collection<PropertyType>): List<String> {
        return SELECT_COLS_FOR_ASSEMBLY + propertyTypes.map {
            quote(it.type.fullQualifiedNameAsString)
        }
    }

    /**
     * Connects to a database using [connection], generates grant select sql on [tableName]
     * using [authorizedPropertyTypesOfPrincipals] in a single batch
     */
    private fun grantSelectForEntitySet(
            connection: Connection,
            tableName: String,
            authorizedPropertyTypesOfPrincipals: Map<Principal, Set<PropertyType>>
    ): IntArray {
        // prepare batch queries
        return connection.createStatement().use { stmt ->
            authorizedPropertyTypesOfPrincipals.forEach { (principal, propertyTypes) ->
                val columns = getSelectColumnsForMaterializedView(propertyTypes)
                try {
                    val grantSelectSql = grantSelectSql(tableName, principal, columns)
                    stmt.addBatch(grantSelectSql)
                } catch (e: IllegalArgumentException) {
                    logger.error("Error granting select for $principal on entity set named $tableName while assembling", e)
                }
            }
            stmt.executeBatch()
        }
    }

    private fun grantSelectForEdges(
            stmt: Statement, tableName: String, entitySetIds: Set<UUID>, authorizedPrincipals: Set<Principal>
    ): IntArray {
        authorizedPrincipals.forEach {
            try {
                val grantSelectSql = grantSelectSql(tableName, it, listOf())
                stmt.addBatch(grantSelectSql)
            } catch (e: NoSuchElementException) {
                logger.error("Principal $it does not exists but has permission on one of the entity sets $entitySetIds")
            }
        }

        return stmt.executeBatch()
    }

    private fun grantSelectForNewMembers(
            connection: Connection,
            authorizedPropertyTypesOfEntitySetsByPostgresUser: Map<String, Map<EntitySet, Collection<PropertyType>>>
    ): IntArray {
        // prepare batch queries
        return connection.createStatement().use { stmt ->
            authorizedPropertyTypesOfEntitySetsByPostgresUser
                    .forEach { (postgresUserName, authorizedPropertyTypesOfEntitySets) ->

                        // grant select on authorized tables and their properties
                        authorizedPropertyTypesOfEntitySets.forEach { (entitySet, propertyTypes) ->
                            val tableName = entitySetNameTableName(entitySet.name)
                            val columns = getSelectColumnsForMaterializedView(propertyTypes)
                            val grantSelectSql = grantSelectSql(tableName, postgresUserName, columns)
                            stmt.addBatch(grantSelectSql)
                        }

                        // also grant select on edges (if at least 1 entity set is materialized to make sure edges
                        // materialized view exist)
                        if (authorizedPropertyTypesOfEntitySets.isNotEmpty()) {
                            val edgesTableName = "$MATERIALIZED_VIEWS_SCHEMA.${E.name}"
                            val grantSelectSql = grantSelectSql(edgesTableName, postgresUserName, listOf())
                            stmt.addBatch(grantSelectSql)
                        }
                    }
            stmt.executeBatch()
        }
    }

    /**
     * Build grant select sql statement for a [entitySetTableName] and [principal] with column level security.
     * If [columns] are left empty, it will grant select on whole table.
     */
    @Throws(IllegalArgumentException::class)
    private fun grantSelectSql(
            entitySetTableName: String,
            principal: Principal,
            columns: List<String>
    ): String {
        val postgresUserName = when (principal.type) {
            PrincipalType.USER -> buildPostgresUsername(securePrincipalsManager.getPrincipal(principal.id))
            PrincipalType.ROLE -> buildPostgresRoleName(securePrincipalsManager.lookupRole(principal))
            else -> throw IllegalArgumentException("Only ${PrincipalType.USER} and ${PrincipalType.ROLE} principal " +
                    "types can be granted select.")
        }

        return grantSelectSql(entitySetTableName, quote(postgresUserName), columns)
    }

    /**
     * Build grant select sql statement for a [tableName] and [postgresUserName] with column level security.
     * If [columns] are left empty, it will grant select on whole table.
     * TODO - should just do whole table if all cols
     */
    private fun grantSelectSql(
            tableName: String,
            postgresUserName: String,
            columns: List<String>
    ): String {
        val onProperties = if (columns.isEmpty()) {
            ""
        } else {
            "( ${columns.joinToString(",")} )"
        }

        return "GRANT SELECT $onProperties " +
                "ON $tableName " +
                "TO $postgresUserName"
    }

    /**
     * Synchronize data changes in entity set materialized view in organization database.
     */
    internal fun refreshEntitySet(organizationId: UUID, entitySet: EntitySet) {
        logger.info("Refreshing entity set ${entitySet.id} in organization $organizationId database")
        val tableName = entitySetNameTableName(entitySet.name)

        connect(buildOrganizationDatabaseName(organizationId)).let { dataSource ->
            dataSource.connection.use { connection ->
                connection.createStatement().use {
                    it.execute("REFRESH MATERIALIZED VIEW $tableName")
                }
            }
        }
    }

    /**
     * Renames a materialized view in the requested organization.
     * @param organizationId The id of the organization in which the entity set is materialized and should be renamed.
     * @param newName The new name of the entity set.
     * @param oldName The old name of the entity set.
     */
    internal fun renameMaterializedEntitySet(organizationId: UUID, newName: String, oldName: String) {
        connect(buildOrganizationDatabaseName(organizationId)).let { dataSource ->
            dataSource.connection.createStatement().use { stmt ->
                val newTableName = quote(newName)
                val oldTableName = entitySetNameTableName(oldName)

                stmt.executeUpdate("ALTER MATERIALIZED VIEW IF EXISTS $oldTableName RENAME TO $newTableName")
            }
        }
        logger.info(
                "Renamed materialized view of entity set with old name $oldName to new name $newName in " +
                        "organization $organizationId"
        )
    }

    /**
     * Removes a materialized entity set from atlas.
     */
    internal fun dematerializeEntitySets(organizationId: UUID, entitySetIds: Set<UUID>) {
        val dbName = buildOrganizationDatabaseName(organizationId)
        connect(dbName).let { dataSource ->
            //TODO: Implement de-materialization code here.
        }
        logger.info("Removed materialized entity sets $entitySetIds from organization $organizationId")
    }

    private val SELECT_COUNT_OF_DATABASE_WITH_NAME_SQL = "select count(*) from pg_database where datname = ?"

    internal fun exists(dbName: String): Boolean {
        target.connection.use { connection ->
            connection.prepareStatement( SELECT_COUNT_OF_DATABASE_WITH_NAME_SQL ).use {ps ->
                ps.setString(1, dbName)
                ps.executeQuery().use {rs ->
                    rs.next()
                    return rs.getInt(COUNT) > 0
                }
            }
        }
    }

    internal fun getAllRoles(): PostgresIterable<Role> {
        return PostgresIterable(
                Supplier {
                    val conn = hds.connection
                    val ps = conn.prepareStatement(PRINCIPALS_SQL)
                    ps.setString(1, PrincipalType.ROLE.name)
                    StatementHolder(conn, ps, ps.executeQuery())
                },
                Function { securePrincipalsManager.getSecurablePrincipal(ResultSetAdapters.aclKey( it )) as Role }
        )
    }

    internal fun getAllUsers(): PostgresIterable<SecurablePrincipal> {
        return PostgresIterable(
                Supplier {
                    val conn = hds.connection
                    val ps = conn.prepareStatement(PRINCIPALS_SQL)
                    ps.setString(1, PrincipalType.USER.name)
                    StatementHolder(conn, ps, ps.executeQuery())
                },
                Function { securePrincipalsManager.getSecurablePrincipal(ResultSetAdapters.aclKey(it)) }
        )
    }

    /**
     * Revokes all role access to the [PUBLIC_SCHEMA] in the database specified by [dataSource]
     */
    private fun revokeAllPublicSchemaAccess(dataSource: HikariDataSource) {
        val roles = getAllRoles()
        if (roles.iterator().hasNext()) {
            val quotedRoleNames = roles.map { quote(buildPostgresRoleName(it)) }
            val roleIdsSql = quotedRoleNames.joinToString(",")

            dataSource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    logger.info("Revoking $PUBLIC_SCHEMA schema right from roles: {}", quotedRoleNames)
                    //Don't allow users to access public schema which will contain foreign data wrapper tables.
                    statement.execute("REVOKE USAGE ON SCHEMA $PUBLIC_SCHEMA FROM $roleIdsSql")
                }
            }
        }
    }

    internal fun createRole(role: Role) {
        val dbRole = buildPostgresRoleName(role)

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(createRoleIfNotExistsSql(dbRole))
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                logger.info("Revoking $PUBLIC_SCHEMA schema right from role: {}", role)
                statement.execute("REVOKE USAGE ON SCHEMA $PUBLIC_SCHEMA FROM ${quote(dbRole)}")

                return@use
            }
        }
    }

    internal fun createUnprivilegedUser(user: SecurablePrincipal) {
        val dbUser = buildPostgresUsername(user)
        //user.name
        val dbUserPassword = dbCredentialService.getOrCreateUserCredentials(dbUser)

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                //TODO: Go through every database and for old users clean them out.
//                    logger.info("Attempting to drop owned by old name {}", user.name)
//                    statement.execute(dropOwnedIfExistsSql(user.name))
//                    logger.info("Attempting to drop user {}", user.name)
//                    statement.execute(dropUserIfExistsSql(user.name)) //Clean out the old users.
//                    logger.info("Creating new user {}", dbUser)
                logger.info("Creating user if not exists {}", dbUser)
                statement.execute(createUserIfNotExistsSql(dbUser, dbUserPassword))
                //Don't allow users to access public schema which will contain foreign data wrapper tables.
                logger.info("Revoking $PUBLIC_SCHEMA schema right from user {}", user)
                statement.execute("REVOKE USAGE ON SCHEMA $PUBLIC_SCHEMA FROM ${quote(dbUser)}")

                return@use
            }
        }
    }

    /**
     * Grants permissions [MEMBER_ORG_DATABASE_PERMISSIONS] on [dbName] to [userPrincipals] in the default database object
     * Grants USAGE on [MATERIALIZED_VIEWS_SCHEMA] to [userPrincipals]
     * Sets search_path for [userPrincipals] to [MATERIALIZED_VIEWS_SCHEMA]
     */
    private fun configureUsersInDatabase(dataSource: HikariDataSource, dbName: String, userPrincipals: Collection<SecurablePrincipal>) {
        val userIds = userPrincipals.map { quote(buildPostgresUsername(it)) }
        val userIdsSql = userIds.joinToString(", ")

        logger.info("Configuring users $userIdsSql in database $dbName")
        //First we will grant all privilege which for database is connect, temporary, and create schema
        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                        "GRANT ${MEMBER_ORG_DATABASE_PERMISSIONS.joinToString(", ")} " +
                                "ON DATABASE ${quote(dbName)} TO $userIdsSql"
                )
            }
        }

        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                logger.info(
                        "Granting usage on $MATERIALIZED_VIEWS_SCHEMA schema and revoking from $PUBLIC_SCHEMA schema for users: $userIds"
                )
                statement.execute("GRANT USAGE ON SCHEMA $MATERIALIZED_VIEWS_SCHEMA TO $userIdsSql")
                //Set the search path for the user
                logger.info("Setting search_path to $MATERIALIZED_VIEWS_SCHEMA for users $userIds")
                userIds.forEach { userId ->
                    statement.addBatch("ALTER USER $userId SET search_path TO $MATERIALIZED_VIEWS_SCHEMA")
                }
                statement.executeBatch()
            }
        }
    }

    private fun revokeConnectAndSchemaUsage(dataSource: HikariDataSource, dbName: String, userIds: List<String>) {
        val userIdsSql = userIds.joinToString(", ")

        logger.info(
                "Removing users $userIds from database $dbName, schema usage and all privileges on all tables in schema $MATERIALIZED_VIEWS_SCHEMA"
        )

        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                        "REVOKE ${MEMBER_ORG_DATABASE_PERMISSIONS.joinToString(", ")} " +
                                "ON DATABASE ${quote(dbName)} FROM $userIdsSql"
                )
                stmt.execute("REVOKE ALL PRIVILEGES ON SCHEMA $MATERIALIZED_VIEWS_SCHEMA FROM $userIdsSql")
                stmt.execute(
                        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA $MATERIALIZED_VIEWS_SCHEMA FROM $userIdsSql"
                )
            }
        }
    }

    val revokeAll = "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA $MATERIALIZED_VIEWS_SCHEMA FROM ?"
}

private val MEMBER_ORG_DATABASE_PERMISSIONS = setOf("CREATE", "CONNECT", "TEMPORARY", "TEMP")
private val PUBLIC_TABLES = setOf(E.name, PROPERTY_TYPES.name, ENTITY_TYPES.name, ENTITY_SETS.name)

private val PRINCIPALS_SQL = "SELECT ${ACL_KEY.name} FROM principals WHERE ${PRINCIPAL_TYPE.name} = ?"

internal fun createRoleIfNotExistsSql(dbRole: String): String {
    return "DO\n" +
            "\$do\$\n" +
            "BEGIN\n" +
            "   IF NOT EXISTS (\n" +
            "      SELECT\n" +
            "      FROM   pg_catalog.pg_roles\n" +
            "      WHERE  rolname = '$dbRole') THEN\n" +
            "\n" +
            "      CREATE ROLE ${quote(
                    dbRole
            )} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOLOGIN;\n" +
            "   END IF;\n" +
            "END\n" +
            "\$do\$;"
}

internal fun createUserIfNotExistsSql(dbUser: String, dbUserPassword: String): String {
    return "DO\n" +
            "\$do\$\n" +
            "BEGIN\n" +
            "   IF NOT EXISTS (\n" +
            "      SELECT\n" +
            "      FROM   pg_catalog.pg_roles\n" +
            "      WHERE  rolname = '$dbUser') THEN\n" +
            "\n" +
            "      CREATE ROLE ${quote(
                    dbUser
            )} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN ENCRYPTED PASSWORD '$dbUserPassword';\n" +
            "   END IF;\n" +
            "END\n" +
            "\$do\$;"
}

internal fun dropOwnedIfExistsSql(dbUser: String): String {
    return "DO\n" +
            "\$do\$\n" +
            "BEGIN\n" +
            "   IF EXISTS (\n" +
            "      SELECT\n" +
            "      FROM   pg_catalog.pg_roles\n" +
            "      WHERE  rolname = '$dbUser') THEN\n" +
            "\n" +
            "      DROP OWNED BY ${quote(
                    dbUser
            )} ;\n" +
            "   END IF;\n" +
            "END\n" +
            "\$do\$;"
}

internal fun dropUserIfExistsSql(dbUser: String): String {
    return "DO\n" +
            "\$do\$\n" +
            "BEGIN\n" +
            "   IF EXISTS (\n" +
            "      SELECT\n" +
            "      FROM   pg_catalog.pg_roles\n" +
            "      WHERE  rolname = '$dbUser') THEN\n" +
            "\n" +
            "      DROP ROLE ${quote(
                    dbUser
            )} ;\n" +
            "   END IF;\n" +
            "END\n" +
            "\$do\$;"
}
