package com.aerospike.client.configuration.serializers.dynamicconfig;


import com.aerospike.client.Log;
import com.aerospike.client.configuration.serializers.dynamicconfig.primitiveprops.BooleanProperty;

public class DynamicBatchDeleteConfig {
    public BooleanProperty sendKey;
    public BooleanProperty durableDelete;

    public DynamicBatchDeleteConfig() {}

    public void setSendKey(BooleanProperty sendKey) { this.sendKey = sendKey; }

    public void setDurableDelete(BooleanProperty durableDelete) { this.durableDelete = durableDelete; }

    public BooleanProperty getSendKey() { return sendKey; }

    public BooleanProperty getDurableDelete() { return durableDelete; }

    @Override
    public String toString() {
        StringBuffer propsString = new StringBuffer("{");
        try {
            propsString.append(" send_key=").append(sendKey.value).append(", ");
            propsString.append(" durable_delete=").append(durableDelete.value).append(", ");
        } catch (Exception e) {
            Log.error(e.toString());
        } finally {
            return propsString.append("}").toString();
        }
    }
}
