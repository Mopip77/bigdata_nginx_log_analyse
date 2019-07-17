package channel;

import java.util.LinkedList;
import java.util.List;

public class NextStepMessage {

    private List<String> messages;

    private static NextStepMessage ourInstance = new NextStepMessage();

    public static NextStepMessage getInstance() {
        return ourInstance;
    }

    private NextStepMessage() {
        messages = new LinkedList<String>();
    }

    public void write(String message) {
        messages.clear();
        messages.add(message);
    }

    public void append(String message) {
        this.messages.add(message);
    }

    public boolean hasMessage() {
        return messages.size() > 0;
    }

    public String read() {
        return messages.remove(0);
    }
}
