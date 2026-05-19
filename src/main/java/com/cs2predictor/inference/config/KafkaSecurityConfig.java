package com.cs2predictor.inference.config;

public class KafkaSecurityConfig {
    private String securityProtocol = "PLAINTEXT";
    private String saslMechanism;
    private String saslUsername;
    private String saslPassword;

    public boolean isSaslEnabled() {
        return saslMechanism != null && !saslMechanism.isEmpty();
    }

    public String buildJaasConfig() {
        return String.format(
            "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
            saslUsername, saslPassword
        );
    }

    public String getSecurityProtocol() { return securityProtocol; }
    public void setSecurityProtocol(String securityProtocol) { this.securityProtocol = securityProtocol; }

    public String getSaslMechanism() { return saslMechanism; }
    public void setSaslMechanism(String saslMechanism) { this.saslMechanism = saslMechanism; }

    public String getSaslUsername() { return saslUsername; }
    public void setSaslUsername(String saslUsername) { this.saslUsername = saslUsername; }

    public String getSaslPassword() { return saslPassword; }
    public void setSaslPassword(String saslPassword) { this.saslPassword = saslPassword; }
}