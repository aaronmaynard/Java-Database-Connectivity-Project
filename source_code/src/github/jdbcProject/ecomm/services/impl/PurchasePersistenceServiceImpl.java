package github.jdbcProject.ecomm.services.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import github.jdbcProject.ecomm.dao.PurchaseDAO;
import github.jdbcProject.ecomm.dao.impl.PurchaseDaoImpl;
import github.jdbcProject.ecomm.entity.Purchase;
import github.jdbcProject.ecomm.services.PurchasePersistenceService;
import github.jdbcProject.ecomm.services.PurchaseSummary;
import github.jdbcProject.ecomm.util.DAOException;

public class PurchasePersistenceServiceImpl implements PurchasePersistenceService {
    private DataSource dataSource;

    public PurchasePersistenceServiceImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Purchase create(Purchase purchase) throws SQLException, DAOException {
        PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            connection.setAutoCommit(false);
            Purchase purc = purchaseDAO.create(connection, purchase);

            connection.commit();
            return purc;
        }
        catch (Exception exc) {
            connection.rollback();
            throw exc;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public Purchase retrieve(Long id) throws SQLException, DAOException {
        PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
        Connection connection = dataSource.getConnection();

        try {
            connection.setAutoCommit(false);
            Purchase purc = purchaseDAO.retrieve(connection, id);

            connection.commit();
            return purc;
        }
        catch (Exception exc) {
            connection.rollback();
            throw exc;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public int update(Purchase purchase) throws SQLException, DAOException {
        PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            connection.setAutoCommit(false);
            int rows = purchaseDAO.update(connection, purchase);
            
            connection.commit();
            return rows;
        }
        catch (Exception exc) {
            connection.rollback();
            throw exc;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public int delete(Long id) throws SQLException, DAOException {
        PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            connection.setAutoCommit(false);
            int rows = purchaseDAO.delete(connection, id);
            
            if (purchaseDAO.retrieveForCustomerID(connection, id) == null) {
                throw new DAOException("No Purchases found");
            }

            connection.commit();
            return rows;
        }
        catch (Exception exc) {
            connection.rollback();
            throw exc;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public List<Purchase> retrieveForCustomerID(Long customerID) throws SQLException, DAOException {
        PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            List<Purchase> purc = purchaseDAO.retrieveForCustomerID(connection, customerID);
            return purc;
        }
        catch (Exception exc) {
            connection.rollback();
            throw exc;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public PurchaseSummary retrievePurchaseSummary(Long customerID) throws SQLException, DAOException {
        PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            PurchaseSummary purc = purchaseDAO.retrievePurchaseSummary(connection, customerID);
            return purc;
        }
        catch (Exception exc) {
            connection.rollback();
            throw exc;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public List<Purchase> retrieveForProductID(Long productID) throws SQLException, DAOException {
        PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            List<Purchase> purc = purchaseDAO.retrieveForProductID(connection, productID);

            if(purc.size() == 0)
                return null;

            return purc;
        }
        catch (Exception exc) {
            connection.rollback();
            throw exc;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

}
