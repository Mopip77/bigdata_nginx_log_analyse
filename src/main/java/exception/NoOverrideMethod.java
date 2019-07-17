package exception;

import java.io.IOException;

public class NoOverrideMethod extends IOException {
    public NoOverrideMethod() {
        super("you should override this method!");
    }
}
