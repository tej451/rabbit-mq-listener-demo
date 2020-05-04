package com.example.demo;

import com.google.common.base.Strings;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.ErrorHandler;

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableRabbit
@Slf4j
public class RabbitMQConfig {

    private static final int MAX_INVERVAL = 10000;

    private static final double BACK_OFF_MULTIPLIER = 10.0;

    private static final int INITIAL_INTERVAL = 500;

    private static final int MAX_ATTEMPTS = 3;

    private static final String SSL_PROTOCOL = "TLSv1.2";

    @Value("${rabbitMQ.test.host}")
    private String testHost;

    @Value("${rabbitMQ.test.virtualHost}")
    private String testVirtualHost;

    @Value("${rabbitMQ.test.username}")
    private String testUsername;

    @Value("${rabbitMQ.test.password}")
    private String testPassword;

    @Value("${rabbitMQ.test.port}")
    private int testPort;

    @Value("${rabbitMQ.test.sslEnabled}")
    private boolean testSSLEnabled;
    
    @Value("${rabbitMQ.test.request.queue}")
    private String testRequestQueue;

    //@Value("${rabbitMQ.test.retry.queue}")
    //private String testRetryQueue;
    
    @Value("${rabbitMQ.listener.Enabled}")
    private boolean isListenerEnabled;


    public RabbitMQConfig() {
        log.info("RabbitMQConfig starting");
    }

    @Bean
    public ConnectionFactory rabbitConnectionFactory() throws URISyntaxException {
        Map<Object, ConnectionFactory> factories = new HashMap<>();
        ConnectionFactory testConnectionFactory = testConnectionFactory();
        factories.put(getFactoryKey(testRequestQueue), testConnectionFactory);
        //factories.put(getFactoryKey(testRetryQueue), testConnectionFactory);
        SimpleRoutingConnectionFactory connectionFactory = new SimpleRoutingConnectionFactory();
        connectionFactory.setTargetConnectionFactories(factories);
        connectionFactory.setDefaultTargetConnectionFactory(testConnectionFactory);
        return connectionFactory;
    }

    private String getFactoryKey(String key) {
        return String.format("[%s]", key);
    }

    public ConnectionFactory testConnectionFactory() throws URISyntaxException {
        log.info("assormentConnection factory startup");
        ConnectionFactory cf = rabbitMQConnectionFactory(testHost, testUsername,
                testPassword, testPort, testVirtualHost, testSSLEnabled);
        log.info("assormentConnection factory done");
        return cf;
    }

    private ConnectionFactory rabbitMQConnectionFactory(String host, String userName, String password, int port,
                                                        String virtualHost, boolean sslEnabled){
        CachingConnectionFactory cf = null;
        if(sslEnabled) {
            com.rabbitmq.client.ConnectionFactory connectionFactory = new com.rabbitmq.client.ConnectionFactory();
            try {
                connectionFactory.useSslProtocol(SSL_PROTOCOL);
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                throw new BeanCreationException("Failed to create Rabbit MQ connection factory", e);
            }
            cf = new CachingConnectionFactory(connectionFactory);
        } else{
            cf = new CachingConnectionFactory();
        }
        cf.setAddresses(host);
        cf.setUsername(userName);
        cf.setPassword(password);
        log.info("RabbitMQ Connection factory startup with Hosts: " + host +
                " and UserName: "+ userName);
        cf.setPort(port);
        if (!Strings.isNullOrEmpty(virtualHost)) {
            cf.setVirtualHost(virtualHost);
        }
        return cf;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() throws URISyntaxException {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(rabbitConnectionFactory());
        factory.setErrorHandler(new CustomErrorHandler());
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setAutoStartup(isListenerEnabled);

        return factory;
    }

    @Bean
    public DefaultMessageHandlerMethodFactory defaultHandlerMethodFactory() {
        DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
        MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
        factory.setMessageConverter(converter);
        return factory;
    }

    @Bean
    public RabbitAdmin testRabbitAdmin() throws URISyntaxException {
        return new RabbitAdmin(testConnectionFactory());
    }


    @Bean
    public RabbitTemplate rabbitTemplate() throws URISyntaxException {
        RabbitTemplate template = new RabbitTemplate(rabbitConnectionFactory());
        template.setMandatory(true);
        template.setChannelTransacted(true);
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
        template.setMessageConverter(converter);
        template.setRetryTemplate(retryTemplate());
        return template;
    }

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(MAX_ATTEMPTS);
        retryTemplate.setRetryPolicy(simpleRetryPolicy);
        retryTemplate.setBackOffPolicy(backOffPolicy());
        return retryTemplate;
    }

    @Bean
    public ExponentialBackOffPolicy backOffPolicy() {
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(INITIAL_INTERVAL);
        backOffPolicy.setMultiplier(BACK_OFF_MULTIPLIER);
        backOffPolicy.setMaxInterval(MAX_INVERVAL);
        return backOffPolicy;
    }

    @Slf4j
    @NoArgsConstructor
    public static class CustomErrorHandler implements ErrorHandler {

        @Override
        public void handleError(Throwable throwable) {
            if (throwable instanceof ListenerExecutionFailedException) {
                ListenerExecutionFailedException e = (ListenerExecutionFailedException) throwable;
                log.error(e.getCause().getMessage());
            }
        }
    }
}
