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
public class Book {
    private String name;
    private String description;
    private String authorKey;
    private Long id;

    public Book(String name, String description, String authorKey, Long id) {
        this.name = name;
        this.description = description;
        this.authorKey = authorKey;
        this.id = id;
    }

    public Book() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getAuthorKey() {
        return authorKey;
    }

    public void setAuthorId(String authorKey) {
        this.authorKey = authorKey;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
    
    public static List<Book> randomizeBooks(List<Author> authors){
        List<Book> books = new ArrayList<Book>();
        Faker f = new Faker();
        for (Author a : authors) {
            books.add(new Book(f.sentence(2), f.sentence(5), a.getKey(), a.getId()));
        }
        return books;
    }
    
    public String getKey(){
        return Joiner.on(":").join("book",id);
    }

    @Override
    public String toString() {
        return "Book{" + "name=" + name + ", description=" + description + ", authorKey=" + authorKey + ", id=" + id + '}';
    }
    
    
}
