package io.digdag.server.rs;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.api.RestSession;

import java.util.List;

@JsonDeserialize(as = PaginationResource.class)
public class PaginationResource {
  private List<RestSession> sessions;
  private int pageCount;

  public PaginationResource(List<RestSession> sessions, int pageCount) {
    this.sessions = sessions;
    this.pageCount = pageCount;
  }

  public List<RestSession>  getSessions() {
    return this.sessions;
  }

  public int getPageCount() {
    return this.pageCount;
  }

}
