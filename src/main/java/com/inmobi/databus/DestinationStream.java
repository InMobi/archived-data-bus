package com.inmobi.databus;

public class DestinationStream {
  private final int retentionInDays;
  private final String name;
  private final Boolean isPrimary;

  public DestinationStream(String name, int retentionInDays, Boolean isPrimary) {
    this.name = name;
    this.retentionInDays = retentionInDays;
    this.isPrimary = isPrimary;
  }

  public boolean isPrimary() {
    return isPrimary;
  }

  public String getName() {
    return name;
  }

  public int getRetentionInDays() {
    return retentionInDays;
  }
}
