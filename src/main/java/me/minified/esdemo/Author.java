/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package me.minified.esdemo;

import com.github.javafaker.Faker;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.common.base.Joiner;

/**
 *
 * @author agung
 */

public class Author {
    
    private Long id;
    private String name;
    private String publisher;

    public Author(Long id, String name, String publisher) {
        this.id = id;
        this.name = name;
        this.publisher = publisher;
    }

    public Author() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }
    
    public static List<Author> randomizeAuthors(int size){
        List<Author> authors = new ArrayList<Author>();
        Faker f = new Faker();
        for (int i = 1; i <= size; i++) {
            Long id = (long)i;
            authors.add(new Author(id, f.name(), f.country()));
        }
        return authors;
    }
    
    public String getKey(){
        return Joiner.on(":").join("author", id);
    }

    @Override
    public String toString() {
        return "Author{" + "id=" + id + ", name=" + name + ", publisher=" + publisher + '}';
    }
    
    
}
