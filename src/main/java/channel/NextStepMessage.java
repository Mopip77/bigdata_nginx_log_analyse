package channel;

public class NextStepMessage {
    private StringBuilder message = new StringBuilder();

    private static NextStepMessage ourInstance = new NextStepMessage();

    public static NextStepMessage getInstance() {
        return ourInstance;
    }

    private NextStepMessage() {
    }

    public void write(String message) {
        this.message = new StringBuilder(message);
    }

    public void append(String message) {
        this.message.append(message);
    }

    public String read() {
        return this.message.toString();
    }
}
