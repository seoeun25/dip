package com.nexr.dip.jpa;


import com.nexr.dip.DipLoaderException;
import com.nexr.dip.server.JDBCService;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.List;

/**
 * Query Executor for the dipproperty.
 */
public class DipPropertyQueryExecutor extends QueryExecutor<DipProperty, DipPropertyQueryExecutor.DipPropertyQuery> {

    public DipPropertyQueryExecutor(JDBCService jdbcService) {
        super(jdbcService);
    }

    public DipProperty get(DipPropertyQuery namedQuery, Object... parameters) throws DipLoaderException {
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        Object ret = jdbcService.executeGet(namedQuery.name(), query, em);
        if (ret == null) {
            throw new DipLoaderException(query.toString());
        }
        DipProperty bean = constructBean(namedQuery, ret, parameters);
        return bean;
    }

    public List<DipProperty> getList(DipPropertyQuery namedQuery, Object... parameters) throws DipLoaderException {
        EntityManager em = jdbcService.getEntityManager();
        Query query = getSelectQuery(namedQuery, em, parameters);
        List<?> retList = (List<?>) jdbcService.executeGetList(namedQuery.name(), query, em);
        List<DipProperty> list = new ArrayList<DipProperty>();
        if (retList != null) {
            for (Object ret : retList) {
                list.add(constructBean(namedQuery, ret));
            }
        }
        return list;
    }

    @Override
    public Query getUpdateQuery(DipPropertyQuery namedQuery, DipProperty bean, EntityManager em) throws DipLoaderException {
        return null;
    }

    public Query getSelectQuery(DipPropertyQuery namedQuery, EntityManager em, Object... parameters)
            throws DipLoaderException {
        Query query = em.createNamedQuery(namedQuery.name());
        switch (namedQuery) {
            case GET_DIPPROPERTY_BY_NAME:
                query.setParameter("name", parameters[0]);
                break;
            case GET_DIPPROPERTY_ALL:
                break;
            default:
                throw new DipLoaderException("DipPropertyQueryExecutor cannot set parameters for " + namedQuery.name());
        }
        return query;
    }

    @Override
    public Object getSingleValue(DipPropertyQuery namedQuery, Object... parameters) throws DipLoaderException {
        return null;
    }

    @Override
    public int executeUpdate(DipPropertyQuery namedQuery, DipProperty jobBean) throws DipLoaderException {
        return 0;
    }

    private DipProperty constructBean(DipPropertyQuery namedQuery, Object ret, Object... parameters)
            throws DipLoaderException {
        DipProperty bean;
        Object[] arr;
        switch (namedQuery) {
            case GET_DIPPROPERTY_BY_NAME:
            case GET_DIPPROPERTY_ALL:
                bean = new DipProperty();
                arr = (Object[]) ret;
                bean.setName((String) arr[0]);
                bean.setValue((String) arr[1]);
                break;
            default:
                throw new DipLoaderException("DipPropertyQueryExecutor cannot construct job bean for " + namedQuery.name());
        }
        return bean;
    }

    public enum DipPropertyQuery {
        GET_DIPPROPERTY_BY_NAME,
        GET_DIPPROPERTY_ALL;
    }
}
