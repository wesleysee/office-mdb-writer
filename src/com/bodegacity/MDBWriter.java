package com.bodegacity;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

public class MDBWriter {

	private static final String PROPERTIES_PATH = "jdbc.properties";
	private static Properties properties = new Properties();

	private static void initProperties() throws FileNotFoundException, IOException {
		properties.load(new FileReader(PROPERTIES_PATH));
	}

	public static void main(String[] args) throws IOException, SQLException {
		initProperties();
		
		Table table = DatabaseBuilder.open(new File("Main_Data.mdb")).getTable("HR_Personnel");

		Set<Integer> employeeIDs = new HashSet<Integer>();
		for (Row row : table) {
			employeeIDs.add(((Number) row.get("Per_Code")).intValue());
		}

		List<Employee> newEmployees = new ArrayList<Employee>();
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = DriverManager.getConnection(
				properties.getProperty("jdbc.url"),
				properties.getProperty("jdbc.username"),
				properties.getProperty("jdbc.password"));
			stmt = conn.createStatement();
			rs = stmt.executeQuery("SELECT * FROM employees");

			while (rs.next()) {
				int id = rs.getInt("id");
				if (!employeeIDs.contains(id)) {
					String name = rs.getString("name");
					newEmployees.add(new Employee(id, name));
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
			if (conn != null) {
				conn.close();
			}
		}

		for (Employee employee : newEmployees) {
			 table.addRow(Column.AUTO_NUMBER, 1, null, "Bodega City", employee.name, employee.id,
				 null, 2, 0, null,
				 0, 0, null, 0, 0, null,
				 0, 0, 0, 0, 0, 0,
				 0, 0, 0, 0, 0, 0,
				 0, 0, 0, 0, 0, 0,
				 0, 0, 0, 0, 0, 0,
				 0, 0, 0, 0, 0);
			 System.out.println("added: " + employee.name + " (" + employee.id + ")");
		}
	}

	private static class Employee {
		protected int id;
		protected String name;

		public Employee(int id, String name) {
			this.id = id;
			this.name = name;
		}
	}
}
