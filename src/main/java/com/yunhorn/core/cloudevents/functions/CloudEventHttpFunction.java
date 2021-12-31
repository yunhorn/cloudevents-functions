package com.yunhorn.core.cloudevents.functions;

import java.net.URI;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * pulsar functions of http cloudevents
 */
public class CloudEventHttpFunction implements Function<byte[], byte[]> {

    volatile RestTemplate  restTemplate;

    @Override
    public byte[] process(byte[] input, Context context) {

        String target = context.getUserConfigMap().getOrDefault("target", "").toString();
        context.getLogger().info("cloudevent.target:{}",target);
        if(target.length()==0 || (!target.startsWith("http://") && !target.startsWith("https://"))){
            return null;
        }

        if(restTemplate==null){
            synchronized (restTemplate){
                if(restTemplate==null){
                    HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
                    httpRequestFactory.setConnectionRequestTimeout(Integer.valueOf(context.getUserConfigMap().getOrDefault("connectionRequestTime",3000).toString()));
                    httpRequestFactory.setConnectTimeout(Integer.valueOf(context.getUserConfigMap().getOrDefault("connectTimeout",3000).toString()));
                    httpRequestFactory.setReadTimeout(Integer.valueOf(context.getUserConfigMap().getOrDefault("readTimeout",3000).toString()));
                    restTemplate = new RestTemplate(httpRequestFactory);
                }
            }
        }


        try {
            restTemplate.exchange(RequestEntity.post(URI.create(target)) //
                    .header("ce-id", context.getCurrentRecord().getProperties().getOrDefault("ceId", "ce-id")) //
                    .header("ce-specversion",
                            context.getCurrentRecord().getProperties().getOrDefault("ceSpecversion", "ceSpecversion")) //
                    .header("ce-type", context.getCurrentRecord().getProperties().getOrDefault("ceType", "ceType")) //
                    .header("ce-source", context.getCurrentRecord().getProperties().getOrDefault("ceSource", "ceSource")) //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .body(new String(input)), String.class);
        }catch (Exception e){
            context.getLogger().error("push.cloudevent.failed!",e);
        }
        return input;
    }
}
