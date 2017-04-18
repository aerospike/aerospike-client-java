package com.aerospike.client.policy;

/**
 * Flipkart: we are bounded by PPS in our network, this directly
 * impacts the Query Per Seconds a Node can support.
 * 
 * Idea of RateLimitter is to shield the Aerospike from additional QPS per node
 * and fail fast in case it exceeds the limit.
 * 
 * maxQPS per client will be function of how many clients are accessing the cluster
 * 
 * As per benchamrking if a node can support 80k QPS and there are 40 clients
 * 
 * maxQPSperNodeForClient = 80k/40 = 2k QPS
 * @author anshul.gupta
 *
 */
public final class RateLimitPolicy {
  
  /**
   * This is QPS not TPS, in case of Batch request of we are request n keys per request and are making X such request
   * this number will bound the X, not n*X
   * 
   * null means noRate Limitting is required
   */
  public Integer maxQPSperNodeForClient;

  public RateLimitPolicy(Integer maxQPSperNodeForClient) {
    super();
    this.maxQPSperNodeForClient = maxQPSperNodeForClient;
  }
  
  public RateLimitPolicy() {
    super();
  }
  

}
