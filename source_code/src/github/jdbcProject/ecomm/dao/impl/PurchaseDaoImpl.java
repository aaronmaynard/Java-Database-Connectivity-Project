package github.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import github.jdbcProject.ecomm.dao.PurchaseDAO;
import github.jdbcProject.ecomm.entity.Customer;
import github.jdbcProject.ecomm.entity.Purchase;
import github.jdbcProject.ecomm.services.PurchaseSummary;
import github.jdbcProject.ecomm.util.DAOException;

public class PurchaseDaoImpl implements PurchaseDAO {
    
    private static final String insertSQL = 
            "INSERT INTO PURCHASE (PRODUCT_id, CUSTOMER_id, purchase_date, purchase_amt) VALUES (?, ?, ?, ?);";
    
    @Override
    public Purchase create(Connection connection, Purchase purchase) throws SQLException, DAOException {
         if (purchase.getId() != null) {
                throw new DAOException("Tring to insert Purchase with NON-NULL ID");
            }
         PreparedStatement ps = null;
            try {
                ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
                ps.setLong(1, purchase.getProductID());
                ps.setLong(2, purchase.getCustomerID());
                java.sql.Date date = new java.sql.Date(purchase.getPurchaseDate().getTime());
                ps.setDate(3, date);
                ps.setDouble(4, purchase.getPurchaseAmount());
                ps.executeUpdate();
                
                ResultSet keyRS = ps.getGeneratedKeys();
                keyRS.next();
                int lastKey = keyRS.getInt(1);
                purchase.setId((long) lastKey);
                return purchase;
            }
            finally {
                if (ps != null && !ps.isClosed()) {
                    ps.close();
               
                }
            }
    }
    
    
    
    final static String selectSQL = 
            "SELECT id, PRODUCT_id, CUSTOMER_id, purchase_date, purchase_amt FROM PURCHASE WHERE id = ?";

    @Override
    public Purchase retrieve(Connection connection, Long id) throws SQLException, DAOException {
  
        if (id == null) {
            throw new DAOException("Trying to retrieve Purchase with NULL ID");
        }
        
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(selectSQL);
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                return null;
            }
            
            Purchase purc = new Purchase();
            purc.setId(rs.getLong("id"));
            purc.setProductID(rs.getLong("PRODUCT_id"));
            purc.setCustomerID(rs.getLong("CUSTOMER_id"));
            purc.setPurchaseDate(rs.getDate("purchase_date"));
            purc.setPurchaseAmount(rs.getDouble("purchase_amt"));
            return purc;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    
    final static String updateSQL = 
            "UPDATE PURCHASE SET PRODUCT_id = ?, CUSTOMER_id = ?, purchase_date = ?, purchase_amt = ? WHERE id = ?;";

    @Override
    public int update(Connection connection, Purchase purchase) throws SQLException, DAOException {

        long id = purchase.getId();
        if (purchase.getId() != null) {
            throw new DAOException("Trying to update Purchase with NULL ID");
        }
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(updateSQL);
            ps.setLong(1, purchase.getProductID());
            ps.setLong(2, purchase.getCustomerID());
            java.sql.Date date = new java.sql.Date(purchase.getPurchaseDate().getTime());
            ps.setDate(3, date);
            ps.setDouble(4, purchase.getPurchaseAmount());
            ps.setLong(5, id);
            
            int rows = ps.executeUpdate();
            return rows;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }
    
    
    final static String deleteSQL = 
            "DELETE FROM PURCHASE WHERE id = ?;";

    @Override
    public int delete(Connection connection, Long id) throws SQLException, DAOException {
        
        if (id == null) {
            throw new DAOException("Trying to delete Purchase with NULL ID");
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
    
    final static String SelectCustomerIdSQL = 
            "SELECT id, PRODUCT_id, CUSTOMER_id, purchase_date, purchase_amt " + "FROM PURCHASE WHERE CUSTOMER_id = ?;";
    
    @Override
    public List<Purchase> retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
        
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(SelectCustomerIdSQL);
            ps.setLong(1, customerID);
            ResultSet rs = ps.executeQuery();

            List<Purchase> result = new ArrayList<Purchase>();
            while (rs.next()) {
                Purchase purc = new Purchase();
                purc.setId(rs.getLong("id"));
                purc.setProductID(rs.getLong("PRODUCT_id"));
                purc.setCustomerID(rs.getLong("CUSTOMER_id"));
                purc.setPurchaseDate(rs.getDate("purchase_date"));
                purc.setPurchaseAmount(rs.getDouble("purchase_amt"));
                result.add(purc);
            }
            
            return result;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }

        }
    }
    
    final static String SelectProductIdSQL = 
            "SELECT id, PRODUCT_id, CUSTOMER_id, purchase_date, purchase_amt " + "FROM PURCHASE WHERE PRODUCT_id = ?;";

    @Override
    public List<Purchase> retrieveForProductID(Connection connection, Long productID) throws SQLException, DAOException {
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(SelectProductIdSQL);
            ps.setLong(1, productID);
            ResultSet rs = ps.executeQuery();

            List<Purchase> result = new ArrayList<Purchase>();
            while (rs.next()) {
                Purchase purc = new Purchase();
                purc.setId(rs.getLong("id"));
                purc.setProductID(rs.getLong("PRODUCT_id"));
                purc.setCustomerID(rs.getLong("CUSTOMER_id"));
                purc.setPurchaseDate(rs.getDate("purchase_date"));
                purc.setPurchaseAmount(rs.getDouble("purchase_amt"));
                result.add(purc);
            }
            
            return result;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }

        }
        
    }
    
    final static String SelectPurchaseSummarySQL = 
            "SELECT purchase_amt" + "FROM PURCHASE WHERE CUSTOMER_id = ?;";
    
    @Override
    public PurchaseSummary retrievePurchaseSummary(Connection connection, Long customerID) throws SQLException, DAOException {
        
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(SelectPurchaseSummarySQL);
            ps.setLong(1, customerID);
            ResultSet rs = ps.executeQuery();
            
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            double total = 0;
            double amount = 0;
             
            while (rs.next()) {
                double current = rs.getDouble("purchase_amount");
                
                if (current <= min) {
                    min = current;
                }
                if (current >= max) {
                    max = current;
                }
                
                total = total + current;
                amount++;
            }
            
            if (amount > 0) {
                PurchaseSummary purcSumm = new PurchaseSummary();
                purcSumm.minPurchase = (float) min;
                purcSumm.maxPurchase = (float) max;
                purcSumm.avgPurchase= (float) (total/amount);
                
                return purcSumm;
            }
            else {
                return null;
            }
        }
        
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }
 
}




    



