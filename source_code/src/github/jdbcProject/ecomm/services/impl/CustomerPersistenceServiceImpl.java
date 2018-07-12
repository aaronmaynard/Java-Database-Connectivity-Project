package github.jdbcProject.ecomm.services.impl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import github.jdbcProject.ecomm.dao.AddressDAO;
import github.jdbcProject.ecomm.dao.CreditCardDAO;
import github.jdbcProject.ecomm.dao.CustomerDAO;
import github.jdbcProject.ecomm.dao.impl.AddressDaoImpl;
import github.jdbcProject.ecomm.dao.impl.CreditCardDaoImpl;
import github.jdbcProject.ecomm.dao.impl.CustomerDaoImpl;
import github.jdbcProject.ecomm.entity.Address;
import github.jdbcProject.ecomm.entity.CreditCard;
import github.jdbcProject.ecomm.entity.Customer;
import github.jdbcProject.ecomm.services.CustomerPersistenceService;
import github.jdbcProject.ecomm.util.DAOException;

public class CustomerPersistenceServiceImpl implements CustomerPersistenceService
{
    private DataSource dataSource;

    public CustomerPersistenceServiceImpl(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }
    
    /**
     * This method provided as an example of transaction support across multiple inserts.
     * 
     * Persists a new Customer instance by inserting new Customer, Address, 
     * and CreditCard instances. Notice the transactional nature of this 
     * method which inludes turning off autocommit at the start of the 
     * process, and rolling back the transaction if an exception 
     * is caught. 
     */
    @Override
    public Customer create(Customer customer) throws SQLException, DAOException
    {
        CustomerDAO customerDAO = new CustomerDaoImpl();
        AddressDAO addressDAO = new AddressDaoImpl();
        CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);
            Customer cust = customerDAO.create(connection, customer);
            Long custID = cust.getId();

            if (cust.getAddress() == null) {
                throw new DAOException("Customers must include an Address instance.");
            }
            Address address = cust.getAddress();
            addressDAO.create(connection, address, custID);

            if (cust.getCreditCard() == null) {
                throw new DAOException("Customers must include an CreditCard instance.");
            }
            CreditCard creditCard = cust.getCreditCard();
            creditCardDAO.create(connection, creditCard, custID);

            connection.commit();
            return cust;
        }
        catch (Exception ex) {
            connection.rollback();
            throw ex;
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
    public Customer retrieve(Long id) throws SQLException, DAOException {
        CustomerDAO customerDAO = new CustomerDaoImpl();
        AddressDAO addressDAO = new AddressDaoImpl();
        CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);
            Customer cust = customerDAO.retrieve(connection, id);
            Long custID = cust.getId();
            cust.setAddress(addressDAO.retrieveForCustomerID(connection, custID));  
            cust.setCreditCard(creditCardDAO.retrieveForCustomerID(connection, custID));
            if (cust.getAddress() == null) {
                throw new DAOException("Customers must include an Address instance.");
            }
            if (cust.getCreditCard() == null) {
                throw new DAOException("Customers must include an CreditCard instance.");
            }
            connection.commit();
            return cust;
        }
        catch (Exception ex) {
            connection.rollback();
            throw ex;
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
    public int update(Customer customer) throws SQLException, DAOException {
        CustomerDAO customerDAO = new CustomerDaoImpl();
        AddressDAO addressDAO = new AddressDaoImpl();
        CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);
            int rows = customerDAO.update(connection, customer);
            
            if (customer.getAddress() == null) {
                throw new DAOException("Customers must include an Address instance.");
            }
            addressDAO.create(connection, customer.getAddress(), customer.getId());
            if (customer.getCreditCard() == null) {
                throw new DAOException("Customers must include an CreditCard instance.");
            }
            
            creditCardDAO.create(connection, customer.getCreditCard(), customer.getId());
            connection.commit();
            return rows;
        }
        
        catch (Exception ex) {
            connection.rollback();
            throw ex;
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
        CustomerDAO customerDAO = new CustomerDaoImpl();
        AddressDAO addressDAO = new AddressDaoImpl();
        CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);
            int rows = customerDAO.delete(connection, id);
            
            if (addressDAO.retrieveForCustomerID(connection, id) == null) {
                throw new DAOException("Customers must include an Address instance.");
            }
            addressDAO.deleteForCustomerID(connection, id);

            if (creditCardDAO.retrieveForCustomerID(connection, id) == null) {
                throw new DAOException("Customers must include an CreditCard instance.");
            }
            creditCardDAO.deleteForCustomerID(connection, id);
            connection.commit();
            return rows;
        }
        catch (Exception ex) {
            connection.rollback();
            throw ex;
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
    public List<Customer> retrieveByZipCode(String zipCode) throws SQLException, DAOException {
        CustomerDAO customerDAO = new CustomerDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            List<Customer> cust = customerDAO.retrieveByZipCode(connection, zipCode);

            if(cust.size() == 0)
                return null;

            return cust;
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
    public List<Customer> retrieveByDOB(Date startDate, Date endDate) throws SQLException, DAOException {
        CustomerDAO customerDAO = new CustomerDaoImpl();
        Connection connection = dataSource.getConnection();
        
        try {
            List<Customer> cust = customerDAO.retrieveByDOB(connection, startDate, endDate);

            if(cust.size() == 0)
                return null;

            return cust;
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
