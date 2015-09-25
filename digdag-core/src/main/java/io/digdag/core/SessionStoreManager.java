package io.digdag.core;

public interface SessionStoreManager
{
    SessionStore getSessionStore(int siteId);
}
