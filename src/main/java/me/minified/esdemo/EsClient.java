/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package me.minified.esdemo;

import java.net.InetSocketAddress;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
/**
 *
 * @author agung
 */
public class EsClient {
    private static Client client = null;
    
    public static Client getClient(){
        if(client == null){
            client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("localhost", 9300)));
            return client;
        }else{
            return client;
        }
    }
}
