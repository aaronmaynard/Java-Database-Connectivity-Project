package github.jdbcProject.ecomm.services.impl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import github.jdbcProject.ecomm.dao.ProductDAO;
import github.jdbcProject.ecomm.dao.impl.ProductDaoImpl;
import github.jdbcProject.ecomm.entity.Product;
import github.jdbcProject.ecomm.services.ProductPersistenceService;
import github.jdbcProject.ecomm.services.PurchaseSummary;
import github.jdbcProject.ecomm.util.DAOException;

public class ProductPersistenceServiceImpl implements ProductPersistenceService
{
    private DataSource dataSource;

    public ProductPersistenceServiceImpl(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }
    
    @Override
    public Product create(Product product) throws SQLException, DAOException {
        ProductDAO productDAO = new ProductDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            connection.setAutoCommit(false);
            Product prod = productDAO.create(connection, product);

            connection.commit();
            return prod;
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
    public Product retrieve(Long id) throws SQLException, DAOException {
        ProductDAO productDAO = new ProductDaoImpl();
        Connection connection = dataSource.getConnection();

        try {
            connection.setAutoCommit(false);
            Product prod = productDAO.retrieve(connection, id);

            connection.commit();
            return prod;
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
    public int update(Product product) throws SQLException, DAOException {
        ProductDAO productDAO = new ProductDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            connection.setAutoCommit(false);
            int rows = productDAO.update(connection, product);
            
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
        ProductDAO productDAO = new ProductDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            connection.setAutoCommit(false);
            int rows = productDAO.delete(connection, id);

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
    public List<Product> retrieveByCategory(int category) throws SQLException, DAOException {
        ProductDAO productDAO = new ProductDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            List<Product> prod = productDAO.retrieveByCategory(connection, category);
            return prod;
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
    public Product retrieveByUPC(String UPC) throws SQLException, DAOException {
        ProductDAO productDAO = new ProductDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            Product prod = productDAO.retrieveByUPC(connection, UPC);

            if(prod.getProdUPC() == null)
                return null;

            return prod;
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