package com.nexr.dip.jpa;

import com.google.inject.Inject;
import com.nexr.dip.DipException;
import com.nexr.dip.jpa.JDBCService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import java.util.List;

/**
 * Base class of the QueryExecutor.
 * @param <T>
 * @param <E>
 */
public abstract class QueryExecutor<T, E extends Enum<E>> {

    private static Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);

    protected  JDBCService jdbcService;

    protected QueryExecutor() {

    }

    @Inject
    public void setJdbcService(JDBCService jdbcService) {
        this.jdbcService = jdbcService;
    }

    public void insert(T bean) throws DipException {
        if (bean != null) {
            EntityManager em = jdbcService.getEntityManager();
            try {
                em.getTransaction().begin();
                em.persist(bean);
                em.getTransaction().commit();
            }
            catch (PersistenceException e) {
                throw new DipException(e);
            }
            finally {
                if (em.getTransaction().isActive()) {
                    LOG.warn("insert ended with an active transaction, rolling back");
                    em.getTransaction().rollback();
                }
                if (em.isOpen()) {
                    em.close();
                }
            }
        }
    }

    public abstract T get(E namedQuery, Object... parameters) throws DipException;

    public abstract List<T> getList(E namedQuery, Object... parameters) throws DipException;

    public abstract Query getUpdateQuery(E namedQuery, T bean, EntityManager em) throws DipException;

    public abstract Query getSelectQuery(E namedQuery, EntityManager em, Object... parameters)
            throws DipException;

    public abstract Object getSingleValue(E namedQuery, Object... parameters)
            throws DipException;

    public abstract int executeUpdate(E namedQuery, T jobBean) throws DipException;
}

