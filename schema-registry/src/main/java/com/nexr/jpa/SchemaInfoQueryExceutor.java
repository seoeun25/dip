package com.nexr.jpa;

import com.nexr.dip.DipException;
import com.nexr.dip.jpa.JDBCService;
import com.nexr.dip.jpa.QueryExecutor;
import com.nexr.schemaregistry.SchemaInfo;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class SchemaInfoQueryExceutor extends QueryExecutor<SchemaInfo, SchemaInfoQueryExceutor.SchemaInfoQuery> {

    public SchemaInfoQueryExceutor() {
        super();
    }

    public SchemaInfoQueryExceutor(JDBCService jdbcService) {
        super(jdbcService);
    }

    public SchemaInfo get(SchemaInfoQuery namedQuery, Object... parameters) throws DipException {
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        Object ret = jdbcService.executeGet(namedQuery.name(), query, em);
        if (ret == null) {
            throw new DipException(query.toString());
        }
        SchemaInfo bean = constructBean(namedQuery, ret, parameters);
        return bean;
    }

    public Query getSelectQuery(SchemaInfoQuery namedQuery, EntityManager em, Object... parameters)
            throws DipException {
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
                throw new DipException("QueryExecutor cannot set parameters for " + namedQuery.name());
        }
        return query;
    }

    @Override
    public Object getSingleValue(SchemaInfoQuery namedQuery, Object... parameters) throws DipException {
        return null;
    }

    @Override
    public int executeUpdate(SchemaInfoQuery namedQuery, SchemaInfo jobBean) throws DipException {
        return 0;
    }


    public List<SchemaInfo> getList(SchemaInfoQuery namedQuery, Object... parameters) throws DipException {
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

    @Override
    public Query getUpdateQuery(SchemaInfoQuery namedQuery, SchemaInfo bean, EntityManager em) throws DipException {
        return null;
    }

    public SchemaInfo getListMaxResult1(SchemaInfoQuery namedQuery, Object... parameters) throws DipException {
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) executeGetListMaxResult(namedQuery.name(), query, em, 1);
        List<SchemaInfo> list = new ArrayList<SchemaInfo>();
        if (retList != null) {
            for (Object ret : retList) {
                list.add(constructBean(namedQuery, ret));
            }
        }
        return list.size() == 1 ? list.get(0) : null;
    }

    public List<?> executeGetListMaxResult(String namedQueryName, Query query, EntityManager em, int maxResult) throws DipException {

        List<?> resultList = null;
        try {
            resultList = query.setMaxResults(maxResult).getResultList();
        } catch (NoResultException e) {
            // return null when no matched result
        }
        return resultList;
    }

    public Object insertR(SchemaInfo schemaInfo) throws DipException {
        EntityManager em = jdbcService.getEntityManager();
        try {
            em.getTransaction().begin();
            em.persist(schemaInfo);
            if (em.getTransaction().isActive()) {
                em.getTransaction().commit();
            }
            return schemaInfo.getId();
        } catch (PersistenceException e) {
            throw new DipException(e);
        } finally {
            try {
                if (em.getTransaction().isActive()) {
                    em.getTransaction().rollback();
                }
            } catch (Exception ex) {
            }
            try {
                if (em.isOpen()) {
                    em.close();
                } else {
                }
            } catch (Exception ex) {
            }
        }

    }

    private SchemaInfo constructBean(SchemaInfoQuery namedQuery, Object ret, Object... parameters)
            throws DipException {
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
                throw new DipException("QueryExecutor cannot construct job bean for " + namedQuery.name());
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
