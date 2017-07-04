package d.crawl4j;

import edu.uci.ics.crawler4j.crawler.CrawlController;

/**
 * Created by rz on 03.06.2016.
 */
public class PostgresCrawlerFactory implements CrawlController.WebCrawlerFactory<PostgresWebCrawler> {

    private final String dbUrl;
    private final String dbUser;
    private final String dbPw;

    public PostgresCrawlerFactory(String dbUrl, String dbUser, String dbPw) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPw = dbPw;
    }

    public PostgresWebCrawler newInstance() throws Exception {
        PostgresDBServiceImpl postgresDBService = new PostgresDBServiceImpl(dbUrl,dbUser,dbPw,"com.mysql.jdbc.Driver");
		return new PostgresWebCrawler(postgresDBService);
    }
}
