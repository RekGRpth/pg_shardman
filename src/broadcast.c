#include "postgres.h"
#include "libpq-fe.h"
#include "executor/spi.h"


#define MAX_COMMANDS 1024
#define MAX_RESP_SIZE 16

Datum
broadcast(PG_FUNCTION_ARGS)
{
	char* sql = PG_GETARG_CSTRING(0);
	bool  ignore_errors = PG_GETARG_BOOL(1);
	bool  two_phase = PG_GETARG_BOOL(2);
	char* sep;
	PGresult *res;
	char* fetch_node_connstr;
	int rc;
	char* conn_str;
	int   n_cmds = 0;
	int   i;
	PGconn conn[MAX_COMMANDS];
	char* resp_buf[MAX_COMMANDS*MAX_RESP_SIZE];
	int resp_size = 0;
	char* errmsg = NULL;

	SPI_XACT_STATUS;
	SPI_PROLOG;

	resp[0] = '\0';

	while ((sep = strchr(sql, ';')) != NULL)
	{
		*sep = '\0';

		rc = sscanf(sql, "%d:%n", &node_id, &n);
		if (rc != 1) {
			elog(ERROR, "Invalid command string: %s", sql);
		}
		sql += n;
		fetch_node_connstr = psprintf("select connstring from nodes where id=%d", node_id);
		if (SPI_exec(fetch_node_connstr, 0) < 0 || SPI_processed != 1)
		{
			elog(ERROR, "Failed ot fetch connection string for node %d", node_dsql);
		}
		pfree(fetch_node_connstr);

		conn_str = SPI_getval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		conn[n_cmds] = PQconnectdb(conn_str);
		if (PQstatus(conn[n_cmds++]) != CONNECTION_OK)
		{
			if (ignore_error)
			{
				errmsg = psprintf("%s%d:Connection failure: %s;", errmsg ? errmsg : "", node_id, PQerrorMessage(conn[i]));
				continue;
			}
			errmsg = psprintf("Failed to connect to node %d: %s", node_id, PQerrorMessage(conn[i]));
			goto cleanup;
		}
		if (two_phase)
		{
			sql = psprintf("BEGIN; %s; PREPARE TRANSACTION 'shardlord';", sql);
		}
		if (!PQsendQuery(conn[n_cmds-1], sql))
		{
			if (ignore_error)
			{
				errmsg = psprintf("%s%d:Failed to send query '%s': %s'", errmsg ? errmsg : "", node_id, sql, node_id, PQerrorMessage(conn[i]));
				continue;
			}
			errmsg = psprintf("Failed to send query '%s' to node %d: %s'", sql, node_id, PQerrorMessage(conn[i]));
			goto cleanup;
		}
		if (two_phase)
		{
			pfree(sql);
		}
		sql = sep + 1;
	}
	for (i = 0; i < n_cmds; i++)
	{
		PGresult* res = PQgetResult(conn[i]);
		if (res != NULL)
		{
			ExecStatusType status = PQresultStatus(res);
			if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK)
			{
				if (ignore_error)
				{
					errmsg = psprintf("%s%d:Command %s failed: %s", errmsg ? errmsg : "", node_id, sql, PQerrorMessage(conn[i]));
					PQclear(res);
					continue;
				}
				errmsg = psprintf("Command %s failed at node %d: %s", sql, node_id, PQerrorMessage(conn[i]));
				PQclear(res);
				goto cleanup;
			}
			if (status == PGRES_TUPLES_OK)
			{
				if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
				{
					if (ignore_error)
					{
						resp_size += sprintf(&resp_buf[resp_size], "?;");
						shmn_elog(WARNING, "Query '%s' doesn't return single tuple at node %d", sql, node_id);
					}
					else
					{
						errmsg = psprintf("Query '%s' doesn't return single tuple at node %d", sql, node_id);
						PQclear(res);
						goto cleanup;
					}
				}
				else
				{
					resp_size += sprintf(&resp_buf[resp_size], "%s;", PQgetvalue(res, 0, 0));
				}
			}
			else
			{
				resp_size += sprintf(&resp_buf[resp_size], "%s;", PQntuples(res));
			}
			PQclear(res);
			res = PQgetResult(conn);
			Assert(res == NULL);
		}
		else
		{
			if (ignore_error)
			{
				errmsg = psprintf("%s%d:Failed to received response for '%s': %s", errmsg ? errmsg : "", node_id, sql, PQerrorMessage(conn[i]));
				continue;
			}
			errmsg = "Failed to receive response for query %s from node %d: %s", sql, node_id, PQerrorMessage(conn[i]);
			goto cleanup;
		}
	}
  cleanup:
	for (i = 0; i < n_cmds; i++)
	{
		if (conn[i])
			continue;
		if (errmsg)
		{
			res = PQexec(conn[i], "ROLLBACK PREPARED 'shardlord'");
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				shmn_elog(WARNING, "Rollback of 2PC failed at node %d: %s", node_id, PQerrorMessage(conn[i]));
			}
			PQclear(res);
		}
		else
		{
			res = PQexec(conn[i], "COMMIT PREPARED 'shardlord'");
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				shmn_elog(WARNING, "Commit of 2PC failed at node %d: %s", node_id, PQerrorMessage(conn[i]));
			}
			PQclear(res);
		}
		PQfinish(conn[i]);
	}

	if (errmsg)
	{
		if (ignore_error)
		{
			resp = psprintf("Error:%s", errmsg);
			shmn_elog(WARNIGN, errmsg);
		}
		else
		{
			shmn_elog(ERROR, errmsg);
		}
	}

	SPI_EPILOG;

	PG_RETURN_CSTRING(resp);
}

