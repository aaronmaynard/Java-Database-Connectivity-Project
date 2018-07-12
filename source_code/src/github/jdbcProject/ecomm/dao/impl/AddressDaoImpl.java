package github.jdbcProject.ecomm.dao.impl;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import github.jdbcProject.ecomm.dao.AddressDAO;
import github.jdbcProject.ecomm.entity.Address;
import github.jdbcProject.ecomm.entity.CreditCard;
import github.jdbcProject.ecomm.util.DAOException;

public class AddressDaoImpl implements AddressDAO
{

	private static final String insertSQL =
			"INSERT INTO ADDRESS (address1, address2, city, state, zipcode, CUSTOMER_id) VALUES (?, ?, ?, ?, ?, ?)";
	
	@Override
	public Address create(Connection connection, Address address, Long customerID) throws SQLException, DAOException {
		// Start writing yer code here
		if (customerID == null) {
			throw new DAOException("Tring to insert Address with NULL CustomerID");
		}
		
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(insertSQL);
			ps.setString(1, address.getAddress1());
			ps.setString(2, address.getAddress2());
			ps.setString(3, address.getCity());
			ps.setString(4, address.getState());
			ps.setString(5, address.getZipcode());
			ps.setLong(6, customerID);
			ps.executeUpdate();
			return address;
		}
		finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
	}

	private static String selectSQL = 
			"SELECT address1, address2, city, state, zipcode, CUSTOMER_id FROM ADDRESS WHERE customerID = ?";

	@Override
	public Address retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		// Start writing yer code here
		if (customerID == null) {
			throw new DAOException("Trying to retrieve Address information without customerID");
		}
		
		PreparedStatement ps = null;
		try {
            ps = connection.prepareStatement(selectSQL);
            ps.setLong(1, customerID);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                return null;
            }
            
            Address addr = new Address();
            addr.setAddress1(rs.getString("address1"));
            addr.setAddress2(rs.getString("address2"));
            addr.setCity(rs.getString("city"));
            addr.setState(rs.getString("state"));
            addr.setZipcode(rs.getString("zipcode"));
            return addr;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
	}
	
	final static String deleteSQL = "DELETE FROM ADDRESS WHERE customerID = ?";

	@Override
	public void deleteForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		// Start writing yer code here
		if (customerID == null) {
            throw new DAOException("Trying to update ADDRESS with NULL customerID");
        }
		
		PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(deleteSQL);
            ps.setLong(1, customerID);
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
	}

}
