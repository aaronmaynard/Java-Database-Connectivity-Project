package github.jdbcProject.ecomm.dao.impl;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import github.jdbcProject.ecomm.dao.CustomerDAO;
import github.jdbcProject.ecomm.entity.Customer;
import github.jdbcProject.ecomm.util.DAOException;

public class CustomerDaoImpl implements CustomerDAO
{
    private static final String insertSQL = 
            "INSERT INTO CUSTOMER (first_name, last_name, dob, gender, email) VALUES (?, ?, ?, ?, ?);";

    @Override
    public Customer create(Connection connection, Customer customer) throws SQLException, DAOException {
        // Start writing yer code here
        if (customer.getId() != null) {
            throw new DAOException("Tring to insert Customer with NON-NULL ID");
        }
        
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, customer.getFirstName());
            ps.setString(2, customer.getLastName());
            ps.setDate(3, customer.getDob());
            ps.setString(4, String.valueOf(customer.getGender()));
            ps.setString(5, customer.getEmail());
            ps.executeUpdate();
            
            // Copy the assigned ID to the customer instance
            ResultSet keyRS = ps.getGeneratedKeys();
            keyRS.next();
            int lastKey = keyRS.getInt(1);
            customer.setId((long) lastKey);
            return customer;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }
    
    final static String selectSQL = 
            "SELECT id, first_name, last_name, gender, dob, email FROM CUSTOMER WHERE id = ?";

    @Override
    public Customer retrieve(Connection connection, Long id) throws SQLException, DAOException {
        // Start writing yer code here
        if (id == null) {
            throw new DAOException("Trying to retrieve Customer with NULL ID");
        }
        
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(selectSQL);
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                return null;
            }
            
            Customer cust = new Customer();
            cust.setId(rs.getLong("id"));
            cust.setFirstName(rs.getString("first_name"));
            cust.setLastName(rs.getString("last_name"));
            cust.setGender(rs.getString("gender").charAt(0));
            cust.setDob(rs.getDate("dob"));
            cust.setEmail(rs.getString("email"));
            return cust;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }
    
    final static String updateSQL = 
            "UPDATE CUSTOMER SET first_name = ?, last_name = ?, gender = ?, dob = ?, email = ? WHERE id = ?;";

    @Override
    public int update(Connection connection, Customer customer) throws SQLException, DAOException {
        // Start writing yer code here
        long id = customer.getId();
        if (customer.getId() != null) {
            throw new DAOException("Trying to update Customer with NULL ID");
        }
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(updateSQL);
            ps.setString(1, customer.getFirstName());
            ps.setString(2, customer.getLastName());
            ps.setString(3, String.valueOf(customer.getGender()));
            ps.setDate(4, customer.getDob());
            ps.setString(5, customer.getEmail());
            ps.setLong(6, id);
            
            int rows = ps.executeUpdate();
            return rows;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }
    
    final static String deleteSQL = "DELETE FROM CUSTOMER WHERE ID = ?";

    @Override
    public int delete(Connection connection, Long id) throws SQLException, DAOException {
        // Start writing yer code here
        if (id == null) {
            throw new DAOException("Trying to update Customer with NULL ID");
        }
        
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(deleteSQL);
            ps.setLong(1, id);
            
            int rows = ps.executeUpdate();
            return rows;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }
    
    final static String custListByZip =
            "SELECT id, first_name, last_name, gender, dob, email FROM CUSTOMER WHERE zipcode = ?";

    @Override
    public List<Customer> retrieveByZipCode(Connection connection, String zipCode) throws SQLException, DAOException {
        // Start writing yer code here
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(custListByZip);
            ps.setString(1, zipCode);
            ResultSet rs = ps.executeQuery();
            
            List<Customer> result = new ArrayList<Customer>();
            while (rs.next()) {
                Customer cust = new Customer();
                cust.setId(rs.getLong("id"));
                cust.setFirstName(rs.getString("first_name"));
                cust.setLastName(rs.getString("last_name"));
                cust.setGender(rs.getString("gender").charAt(0));
                cust.setDob(rs.getDate("dob"));
                cust.setEmail(rs.getString("email"));
                result.add(cust);
            }
            return result;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    final static String custListByDob =
            "SELECT id, first_name, last_name, gender, dob, email FROM CUSTOMER WHERE start >= ? AND end <= ?";

    @Override
    public List<Customer> retrieveByDOB(Connection connection, Date startDate, Date endDate)
            throws SQLException, DAOException {
        // Start writing yer code here
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(custListByZip);
            ps.setDate(1, startDate);
            ps.setDate(2, endDate);
            ResultSet rs = ps.executeQuery();
            
            List<Customer> result = new ArrayList<Customer>();
            while (rs.next()) {
                Customer cust = new Customer();
                cust.setId(rs.getLong("id"));
                cust.setFirstName(rs.getString("first_name"));
                cust.setLastName(rs.getString("last_name"));
                cust.setGender(rs.getString("gender").charAt(0));
                cust.setDob(rs.getDate("dob"));
                cust.setEmail(rs.getString("email"));
                result.add(cust);
            }
            return result;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }
    
}
