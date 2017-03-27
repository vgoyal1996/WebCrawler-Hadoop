import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class CrawlerReducer extends Reducer<Text,Text,Text,Text> {
	public static final int MAX_PAGES_TO_SEARCH = 50;
	public Set<String> pagesVisited = new HashSet<>();
	public List<String> pagesToVisit = new LinkedList<>();
	public static final String URL_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.112 Safari/535.1";
	public List<String> links = new LinkedList<>();
	public Document htmlDocument;
	
	public String search(String url,String searchWord){
		String currentUrl = url;
		CrawlerReducer leg = new CrawlerReducer();
		leg.crawl(currentUrl);
		boolean success = leg.searchForWord(searchWord);
		this.pagesToVisit.addAll(leg.getLinks());
		if(success){
			return String.format("Success, Word %s found %s", searchWord,currentUrl);
		}
		else
			return String.format("Failed, Word %s not found %b", searchWord,success);
	}
	
	private String nextUrl(){
		String nextUrl;
		do{
			nextUrl = this.pagesToVisit.remove(0);
		}while(this.pagesVisited.contains(nextUrl));
		this.pagesVisited.add(nextUrl);
		return nextUrl;
	}
	
	public boolean crawl(String url){
		try{
			Connection connection = Jsoup.connect(url).userAgent(URL_AGENT);
			Document htmlDocument = connection.get();
			this.htmlDocument = htmlDocument;
			if(!connection.response().contentType().contains("text/html"))
				return false;
			Elements linksOnPage = htmlDocument.select("a[href]");
			for(Element e : linksOnPage){
				this.links.add(e.absUrl("href"));
			}
			return true;
		}catch(IOException e){
			return false;
		}
	}
	
	public boolean searchForWord(String searchWord){
		if(this.htmlDocument == null)
			return false;
		String bodyText = this.htmlDocument.body().text();
		//return StringUtils.countMatches(bodyText.toLowerCase(),searchWord.toLowerCase());
		return bodyText.toLowerCase().contains(searchWord.toLowerCase());
	}
	
	public List<String> getLinks(){
		return this.links;
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException,InterruptedException{
		for(Text value : values){
			CrawlerReducer spider = new CrawlerReducer();
			String key1 = key.toString();
			String value1 = value.toString();
			while(spider.pagesVisited.size()<MAX_PAGES_TO_SEARCH){
				if(spider.pagesToVisit.isEmpty())
					spider.pagesVisited.add(key1);
				else
					key1 = spider.nextUrl();
				String result = spider.search(key1,value1);
				Text result1 = new Text(result);
				int r;
				if(result.startsWith("S")){
					r=1;
				}
				else
					r=0;
				String str = Integer.toString(r);
				Text r1 = new Text(str);
				context.write(result1, r1);
			}
			String finalResult = String.format("Done, visited %s web pages", spider.pagesVisited.size());
			Text finalText = new Text(finalResult);
			context.write(finalText, new Text(Integer.toString(-1)));
		}
	}
	
}
