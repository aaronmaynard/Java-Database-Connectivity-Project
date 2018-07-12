package github.jdbcProject.ecomm.dao.impl;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import github.jdbcProject.ecomm.dao.CreditCardDAO;
import github.jdbcProject.ecomm.entity.CreditCard;
import github.jdbcProject.ecomm.entity.Customer;
import github.jdbcProject.ecomm.util.DAOException;


public class CreditCardDaoImpl implements CreditCardDAO
{

	private static final String insertSQL =
			"INSERT INTO CREDIT_CARD (name, cc_number, exp_date, security_code, CUSTOMER_id) VALUES (?, ?, ?, ?, ?)";
	
	@Override
	public CreditCard create(Connection connection, CreditCard creditCard, Long customerID)
			throws SQLException, DAOException {
		// Start writing yer code here
		if (customerID == null) {
			throw new DAOException("Tring to insert Credit Card with NULL CustomerID");
		}
		
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(insertSQL);
			ps.setString(1, creditCard.getName());
			ps.setString(2, creditCard.getCcNumber());
			ps.setString(3, creditCard.getExpDate());
			ps.setString(4, creditCard.getSecurityCode());
			ps.setLong(5, customerID);
			ps.executeUpdate();
			return creditCard;
		}
		finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
	}
	
	private static String selectSQL = 
			"SELECT name, cc_number, exp_date, security_code, CUSTOMER_id FROM CREDIT_CARD WHERE customerID = ?";

	@Override
	public CreditCard retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		// Start writing yer code here
		if (customerID == null) {
			throw new DAOException("Trying to retrieve Credit Card information without customerID");
		}
		
		PreparedStatement ps = null;
		try {
            ps = connection.prepareStatement(selectSQL);
            ps.setLong(1, customerID);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                return null;
            }
            
            CreditCard cc = new CreditCard();
            cc.setName(rs.getString("name"));
            cc.setCcNumber(rs.getString("cc_number"));
            cc.setExpDate(rs.getString("exp_date"));
            cc.setSecurityCode(rs.getString("security_code"));
            return cc;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
	}

    final static String deleteSQL = "DELETE FROM CREDIT_CARD WHERE customerID = ?";
	
	@Override
	public void deleteForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		// Start writing yer code here
		if (customerID == null) {
            throw new DAOException("Trying to update CREDIT_CARD with NULL customerID");
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
