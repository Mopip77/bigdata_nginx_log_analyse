package channel;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class NextStepMessageMap {

    private Map<Integer, String> messageMap;

    private static NextStepMessageMap ourInstance = new NextStepMessageMap();

    public static NextStepMessageMap getInstance() {
        return ourInstance;
    }

    private NextStepMessageMap() {
        messageMap = new HashMap<>();
    }

    public void clear() {
        messageMap.clear();
    }

    public void write(Integer key, String message) {
        messageMap.put(key, message);
    }

    public String read(Integer key) {
        if (messageMap.containsKey(key))
            return messageMap.get(key);
        else
            return null;
    }
}
