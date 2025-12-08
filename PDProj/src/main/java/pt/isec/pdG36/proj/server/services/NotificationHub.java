package pt.isec.pdG36.proj.server.services;

import pt.isec.pdG36.proj.server.db.User;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class NotificationHub {
    private final Set<PrintWriter> studentNotificationSinks =
            Collections.synchronizedSet(new HashSet<>());



    public void registerStudentNotification(User user, PrintWriter out) {
        if (user != null && "STUDENT".equalsIgnoreCase(user.role())) {
            studentNotificationSinks.add(out);
        }
    }

    public void unregisterNotification(PrintWriter out) {
        studentNotificationSinks.remove(out);
    }

    public void notifyStudents(String payload) {
        String line = "NOTIFY " + payload;
        synchronized (studentNotificationSinks) {
            for (PrintWriter pw : studentNotificationSinks) {
                try {
                    pw.println(line);
                } catch (Exception ignored) {
                    // broken clients will be cleaned up on close
                }
            }
        }
    }
}
