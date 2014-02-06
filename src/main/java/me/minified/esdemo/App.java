package me.minified.esdemo;

import java.util.List;
import java.util.Map;
import org.elasticsearch.client.Client;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Client client = EsClient.getClient();
        
        final List<Author> authors = Author.randomizeAuthors(10);
        final List<Book> books = Book.randomizeBooks(authors);
        
        final EsDao esd = new EsDao(client); 
        
        indexTask(esd);
        System.out.println("index author");
        for (Author au : authors) {
            esd.indexParent(au);
        }
        System.out.println("index book");
        for (int i = 0; i < books.size(); i++) {
            Book b = books.get(i);
            Author au = authors.get(i);
            esd.indexChild(b, au);
        }
        
        for(Map<String, Object> m : esd.searchBook("walter")){
            System.out.println("found = "+m.toString());
        }
    }
    
    private static void indexTask(EsDao esd){
        esd.removeIndices();
        esd.createIndex();
    }
}
