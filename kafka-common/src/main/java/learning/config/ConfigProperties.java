package learning.config;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * Class representing config properties and it will be injected via guice.
 *
 * @author : Vivek Kumar Gupta
 * @since : 20/07/20
 */
@NoArgsConstructor
@Data
@Getter
public final class ConfigProperties {

    private String consumer_key;
    private String consumer_secret;
    private String token;
    private String secret;

    @Inject
    public ConfigProperties(@Named("consumerKey") String consumer_key,
                            @Named("consumerSecret") String consumer_secret,
                            @Named("token") String token,
                            @Named("secret") String secret){
        this.consumer_key = consumer_key;
        this.consumer_secret= consumer_secret;
        this.token = token;
        this.secret = secret;
    }






}
