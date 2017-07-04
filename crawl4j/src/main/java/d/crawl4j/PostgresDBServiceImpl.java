package d.crawl4j;

import java.beans.PropertyVetoException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.parser.HtmlParseData;

public class PostgresDBServiceImpl implements PostgresDBService {

	private static final Logger logger = org.slf4j.LoggerFactory.getLogger(PostgresDBServiceImpl.class);

	private ComboPooledDataSource comboPooledDataSource;

	private PreparedStatement insertKeyStatement;

	public PostgresDBServiceImpl(String dbUrl, String dbUser, String dbPw, String driver)
			throws PropertyVetoException, SQLException {
		comboPooledDataSource = new ComboPooledDataSource();
		comboPooledDataSource.setDriverClass(driver);
		comboPooledDataSource.setJdbcUrl(dbUrl);
		comboPooledDataSource.setUser(dbUser);
		comboPooledDataSource.setPassword(dbPw);

		createDatabase();
	}

	private void createDatabase() throws SQLException {

		;
		/*comboPooledDataSource.getConnection().createStatement().executeUpdate(
		        "CREATE TABLE webpage" +
		                " ( "+
		                "  html TEXT," +
		                "  text TEXT," +
		                "  url varchar(4096)" +
		                ")");*/

		insertKeyStatement = comboPooledDataSource.getConnection()
				.prepareStatement("insert into webpage values " + "(?,?,?)");

	}

	public void store(Page page) {

		if (page.getParseData() instanceof HtmlParseData) {
			try {

				HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();

				insertKeyStatement.setString(1, htmlParseData.getHtml());
				insertKeyStatement.setString(2, htmlParseData.getText());
				insertKeyStatement.setString(3, page.getWebURL().getURL());
				//                insertKeyStatement.setTimestamp(4, new Timestamp(new java.util.Date().getTime()));
				insertKeyStatement.executeUpdate();
			} catch (SQLException e) {
				logger.error("SQL Exception while storing webpage for url'{}'", page.getWebURL().getURL(), e);
				throw new RuntimeException(e);
			}
		}
	}

	public void close() {
		if (comboPooledDataSource != null) {
			comboPooledDataSource.close();
		}
	}
}
