package org.hajecsdb.graphs.restLayer;

import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

@Component
public class SessionPool {

    private Logger logger = Logger.getLogger(this.getClass().getName());

    private Set<Session> sessionPool = new HashSet<>();

    public synchronized Session createSession() {
        Session session = new Session(this, UUID.randomUUID().toString());
        sessionPool.add(session);
        logger.info("New session created: " + session);
        return session;
    }

    public synchronized String closeSession(String sessionId) {
        Session session = new Session(this, sessionId);
        if (sessionPool.contains(session)) {
            sessionPool.remove(session);
            logger.info("Session closed: " + session);
            return "Session closed!";
        }
        return "Session not found!";
    }

    public synchronized boolean isSessionOpen(String sessionId) {
        return sessionPool.contains(new Session(this, sessionId));
    }

    public synchronized Optional<Session> getSession(String sessionId) {
        return sessionPool.stream().filter(session -> session.getSessionId().equals(sessionId)).findFirst();
    }
}
