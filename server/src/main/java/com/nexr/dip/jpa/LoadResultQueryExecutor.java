package com.nexr.dip.jpa;


import com.nexr.dip.DipLoaderException;
import com.nexr.dip.loader.LoadResult;
import com.nexr.dip.server.JDBCService;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Query Executor for the loadresult.
 */
public class LoadResultQueryExecutor extends QueryExecutor<LoadResult, LoadResultQueryExecutor.LoadResultQuery> {

    public LoadResultQueryExecutor(JDBCService jdbcService) {
        super(jdbcService);
    }

    @Override
    public LoadResult get(LoadResultQuery namedQuery, Object... parameters) throws DipLoaderException {
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        Object ret = jdbcService.executeGet(namedQuery.name(), query, em);
        if (ret == null) {
            throw new DipLoaderException(query.toString());
        }
        LoadResult bean = constructBean(namedQuery, ret, parameters);
        return bean;
    }

    @Override
    public List<LoadResult> getList(LoadResultQuery namedQuery, Object... parameters) throws DipLoaderException {
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jdbcService.executeGetList(namedQuery.name(), query, em);
        List<LoadResult> list = new ArrayList<LoadResult>();
        if (retList != null) {
            for (Object ret : retList) {
                list.add(constructBean(namedQuery, ret));
            }
        }
        return list;
    }

    @Override
    public Query getUpdateQuery(LoadResultQuery namedQuery, LoadResult bean, EntityManager em) throws DipLoaderException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case UPDATE_LOADRESULT:
                query.setParameter("statusStr", bean.getStatusStr());
                query.setParameter("jobId", bean.getJobId());
                query.setParameter("wfStatusStr", bean.getWfStatusStr());
                query.setParameter("externalId", bean.getExternalId());
                query.setParameter("etlExecutionPath", bean.getEtlExecutionPath());
                query.setParameter("countFile", bean.getCountFile());
                query.setParameter("count", bean.getCount());
                query.setParameter("errorCount", bean.getErrorCount());
                query.setParameter("error", bean.getError());
                query.setParameter("resultFiles", bean.getResultFiles());
                query.setParameter("name", bean.getName());
                query.setParameter("executionTime", bean.getExecutionTime());
                query.setParameter("endTime", bean.getEndTime());
                break;
            default:
                throw new DipLoaderException("QueryExecutor cannot set parameters for " + namedQuery.name());
        }
        return query;

    }

    @Override
    public Query getSelectQuery(LoadResultQuery namedQuery, EntityManager em, Object... parameters) throws DipLoaderException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_LOADRESULT:
                query.setParameter("name", parameters[0]);
                query.setParameter("executionTime", parameters[1]);
                break;
            case GET_LOADRESULT_BY_TOPIC:
                query.setParameter("name", parameters[0]);
                query.setMaxResults((Integer) parameters[1]);
                break;
            case GET_LOADRESULT_FROM_TIME:
                query.setParameter("executionTime", parameters[0]);
                query.setMaxResults((Integer) parameters[1]);
                break;
            default:
                throw new DipLoaderException("LoadResultQueryExecutor cannot set parameters for " + namedQuery.name());
        }
        return query;
    }

    @Override
    public Object getSingleValue(LoadResultQuery namedQuery, Object... parameters) throws DipLoaderException {
        return null;
    }

    @Override
    public int executeUpdate(LoadResultQuery namedQuery, LoadResult jobBean) throws DipLoaderException {
        EntityManager em = jdbcService.getEntityManager();
        Query query = getUpdateQuery(namedQuery, jobBean, em);

        int ret = jdbcService.executeUpdate(namedQuery.name(), query, em);
        return ret;
    }

    private LoadResult constructBean(LoadResultQuery namedQuery, Object ret, Object... parameters)
            throws DipLoaderException {
        LoadResult bean;
        Object[] arr;
        switch (namedQuery) {
            case GET_LOADRESULT:
            case GET_LOADRESULT_BY_TOPIC:
            case GET_LOADRESULT_FROM_TIME:
                bean = new LoadResult();
                arr = (Object[]) ret;
                bean.setName((String) arr[0]);
                bean.setExecutionTime((Timestamp) arr[1]);
                bean.setStatusStr((String) arr[2]);
                bean.setJobId((String) arr[3]);
                bean.setWfStatusStr((String) arr[4]);
                bean.setExternalId((String) arr[5]);
                bean.setEtlExecutionPath((String) arr[6]);
                bean.setCountFile((String) arr[7]);
                bean.setCount((Long) arr[8]);
                bean.setErrorCount((Long) arr[9]);
                bean.setError((String) arr[10]);
                bean.setResultFiles((String) arr[11]);
                bean.setEndTime((Timestamp) arr[12]);
                break;
            default:
                throw new DipLoaderException("LoadResultQueryExecutor cannot construct job bean for " + namedQuery.name());
        }
        return bean;
    }

    public enum LoadResultQuery {
        GET_LOADRESULT,
        GET_LOADRESULT_BY_TOPIC,
        GET_LOADRESULT_FROM_TIME,
        UPDATE_LOADRESULT;
    }
}
