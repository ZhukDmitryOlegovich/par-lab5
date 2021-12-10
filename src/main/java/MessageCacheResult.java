public class MessageCacheResult {
    private final String url;
    private final long responseTime;

    public MessageCacheResult(String url, long responseTime) {
        this.url = url;
        this.responseTime = responseTime;
    }

    public String getUrl() {
        return url;
    }

    public long getResponseTime() {
        return responseTime;
    }
}
