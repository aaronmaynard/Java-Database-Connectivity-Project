package github.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import github.jdbcProject.ecomm.dao.ProductDAO;
import github.jdbcProject.ecomm.entity.Product;
import github.jdbcProject.ecomm.util.DAOException;

public class ProductDaoImpl implements ProductDAO
{
    
    private static final String insertSQL = "INSERT INTO PRODUCT (id, prod_name, prod_desc, prod_category, prod_upc) VALUES (?, ?, ?, ?, ?)";

    @Override
    public Product create(Connection connection, Product product) throws SQLException, DAOException {
        // Start writing yer code here
        if (product.getId() == null) {
            throw new DAOException("Trying to insert Product with NULL ID");
        }
        
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(insertSQL);
            ps.setLong(1,  product.getId());
            ps.setString(2, product.getProdName());
            ps.setString(3, product.getProdDescription());
            ps.setInt(4, product.getProdCategory());
            ps.setString(5, product.getProdUPC());
            ps.executeUpdate();
            return product;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }
    
    private static String selectSQL = "SELECT id, prod_name, prod_desc, prod_category, prod_upc";

    @Override
    public Product retrieve(Connection connection, Long id) throws SQLException, DAOException {
        // Start writing yer code here
        if(id == null) {
            throw new DAOException("Trying to retrieve Product Information without valid ID");
        }
        
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(selectSQL);
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();
            if(!rs.next())
                return null;
            
            Product prod = new Product();
            prod.setId(rs.getLong("id"));
            prod.setProdName(rs.getString("prod_name"));
            prod.setProdDescription(rs.getString("prod_desc"));
            prod.setProdCategory(rs.getInt("category"));
            prod.setProdUPC(rs.getString("prod_upc"));
            return prod;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }
    
    final static String updateSQL = "UPDATE product SET id = ?, prod_name = ?, prod_desc = ?, category = ?, prod_upc = ?" + "WHERE id = ?";

    @Override
    public int update(Connection connection, Product product) throws SQLException, DAOException {
        // Start writing yer code here
        if(product.getId() == null) {
            throw new DAOException("Trying to update Product with NULL id");
        }
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(updateSQL);
            ps.setLong(1, product.getId());
            ps.setString(1, product.getProdName());
            ps.setString(3, product.getProdDescription());
            ps.setInt(4, product.getProdCategory());
            ps.setString(5, product.getProdUPC());
            
            int rows = ps.executeUpdate();
            return rows;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
        
    }
    
    final static String deleteSQL = "DELETE FROM WHERE ID = ?";
    
    @Override
    public int delete(Connection connection, Long id) throws SQLException, DAOException {
        // Start writing yer code here
        if (id == null) {
            throw new DAOException("Trying to delete PRODUCT with NULL ID");
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
    
    private static String selectSQL2 = "SELECT id, prod_name, prod_desc, prod_category, prod_upc FROM PRODUCT WHERE category = ?";
    
    @Override
    public List<Product> retrieveByCategory(Connection connection, int category) throws SQLException, DAOException {
        // Start writing yer code here
        if(category == 0) {
            throw new DAOException("Trying to retrieve Product Information without category");
        }
        
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(selectSQL2);
            ps.setInt(1, category);
            ResultSet rs = ps.executeQuery();
            if(!rs.next())
                return null;
            
            Product prod = new Product();
            prod.setId(rs.getLong("id"));
            prod.setProdName(rs.getString("prod_name"));
            prod.setProdDescription(rs.getString("prod_desc"));
            prod.setProdCategory(rs.getInt("category"));
            prod.setProdUPC(rs.getString("prod_upc"));
            return (List<Product>) prod;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }
    
    private static String selectSQL3 = "SELECT id, prod_name, prod_desc, prod_category, prod_upc FROM PRODUCT WHERE upc = ?";

    @Override
    public Product retrieveByUPC(Connection connection, String upc) throws SQLException, DAOException {
        // Start writing yer code here
        if(upc == null) {
            throw new DAOException("Trying to retrieve Product Information without valid UPC");
        }
        
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(selectSQL3);
            ps.setString(1, upc);
            ResultSet rs = ps.executeQuery();
            if(!rs.next())
                return null;
            
            Product prod = new Product();
            prod.setId(rs.getLong("id"));
            prod.setProdName(rs.getString("prod_name"));
            prod.setProdDescription(rs.getString("prod_desc"));
            prod.setProdCategory(rs.getInt("category"));
            prod.setProdUPC(rs.getString("prod_upc"));
            return prod;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

}