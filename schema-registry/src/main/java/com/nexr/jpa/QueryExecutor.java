package com.nexr.jpa;

import com.nexr.AvroRepoException;
import com.nexr.schemaregistry.SchemaInfo;
import com.nexr.server.DipSchemaRepoServer;
import com.nexr.server.JDBCService;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class QueryExecutor {

    public Query getSelectQuery(SchemaInfoQuery namedQuery, EntityManager em, Object... parameters)
            throws AvroRepoException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_BYSCHEMA:
                query.setParameter("schemaStr", parameters[0]);
                break;
            case GET_BYID:
                query.setParameter("id", parameters[0]);
                break;
            case GET_BYTOPICANDID:
                query.setParameter("name", parameters[0]);
                query.setParameter("id", parameters[1]);
            case GET_BYTOPICLATEST:
                query.setParameter("name", parameters[0]);
            case GET_ALL:
                break;
            default:
                throw new AvroRepoException("QueryExecutor cannot set parameters for " + namedQuery.name());
        }
        return query;
    }

    public SchemaInfo get(SchemaInfoQuery namedQuery, Object... parameters) throws AvroRepoException {
        JDBCService jdbcService = DipSchemaRepoServer.getInstance().getJdbcService();
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        Object ret = jdbcService.executeGet(namedQuery.name(), query, em);
        if (ret == null) {
            throw new AvroRepoException(query.toString());
        }
        SchemaInfo bean = constructBean(namedQuery, ret, parameters);
        return bean;
    }

    public List<SchemaInfo> getList(SchemaInfoQuery namedQuery, Object... parameters) throws AvroRepoException {
        JDBCService jdbcService = DipSchemaRepoServer.getInstance().getJdbcService();
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jdbcService.executeGetList(namedQuery.name(), query, em);
        List<SchemaInfo> list = new ArrayList<SchemaInfo>();
        if (retList != null) {
            for (Object ret : retList) {
                list.add(constructBean(namedQuery, ret));
            }
        }
        return list;
    }

    public SchemaInfo getListLimit1(SchemaInfoQuery namedQuery, Object... parameters) throws AvroRepoException {
        JDBCService jdbcService = DipSchemaRepoServer.getInstance().getJdbcService();
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jdbcService.executeGetListLimit1(namedQuery.name(), query, em);
        List<SchemaInfo> list = new ArrayList<SchemaInfo>();
        if (retList != null) {
            for (Object ret : retList) {
                list.add(constructBean(namedQuery, ret));
            }
        }
        return list.size() == 1 ? list.get(0) : null;
    }

    public Object insert(SchemaInfo schemaInfo) throws AvroRepoException {
        JDBCService jdbcService = DipSchemaRepoServer.getInstance().getJdbcService();
        return jdbcService.insert(schemaInfo);
    }

    private SchemaInfo constructBean(SchemaInfoQuery namedQuery, Object ret, Object... parameters)
            throws AvroRepoException {
        SchemaInfo bean;
        Object[] arr;
        switch (namedQuery) {
            case GET_BYSCHEMA:
            case GET_BYID:
            case GET_BYTOPICLATEST:
            case GET_BYTOPICANDID:
            case GET_ALL:
                bean = new SchemaInfo();
                arr = (Object[]) ret;
                bean.setName((String) arr[0]);
                bean.setId((Long) arr[1]);
                bean.setSchemaStr((String) arr[2]);
                bean.setCreated((Calendar) arr[3]);
                break;
            default:
                throw new AvroRepoException("QueryExecutor cannot construct job bean for " + namedQuery.name());
        }
        return bean;
    }

    public enum SchemaInfoQuery {
        GET_BYSCHEMA,
        GET_BYID,
        GET_BYTOPICLATEST,
        GET_BYTOPICANDID,
        GET_ALL;
    }
}
