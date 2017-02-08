namespace java com.vrg.thrift

enum Status {
  UP = 1,
  DOWN = 2,
}

service MembershipServiceT {
   void receiveLinkUpdateMessage(1:string src, 2:string dst, 3:Status status, 4:i64 config),
}
