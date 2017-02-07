namespace java com.vrg.thrift

enum Status {
  UP = 1,
  DOWN = 2,
}

struct LinkUpdateMessageT {
  1: string src,
  2: string dst,
  3: Status op,
  4: i64 config,
}

service MembershipServiceT {
   void receiveLinkUpdateMessage(1:LinkUpdateMessageT msg),
}
