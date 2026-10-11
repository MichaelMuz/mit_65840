# MIT 6.5840 Distributed Systems

Learning from [6.5840 (Spring 2026)](https://pdos.csail.mit.edu/6.824/schedule.html)

## Labs

| Lab # | Name             | Part               | Status                    |
|-------|------------------|--------------------|---------------------------|
| 1     | MapReduce        |                    | Done                      |
| 2     | Key/Value server | KV server          | Done                      |
|       |                  | Distributed lock   | Done                      |
| 3     | Raft             | 3A Leader election | Done                      |
|       |                  | 3B Log             | In progress (fast backup) |
|       |                  | 3C Persistence     |                           |
|       |                  | 3D Log compaction  |                           |
| 4     | KV Raft          | 4A to 4C           |                           |
| 5     | Sharded KV       | 5A to 5D           |                           |

2026 lectures inserted lab 2 so in 2020 Raft is lab 2 and everything is shifted down one.

## Running tests

Need to compile code on every change bc tester runs it as a separate process.
Append `-race` to test commands to check for race conditions.

### MapReduce

```sh
(cd src/mrapps && for f in *.go; do go build -buildmode=plugin $f || exit 1; done) && (cd src/main && for p in mrsequential mrcoordinator mrworker; do go build $p.go || exit 1; done) && go test -C src/mr
```

### KV server and distributed lock

```sh
go build -C src/main kvsrv1d.go && go test -C src/kvsrv1 ./...
```

### Raft

Swap `3B` for the part being worked on.

```sh
go build -C src/main raft1d.go && go test -C src/raft1 -run 3B
```

## Lectures

The only recorded lectures are from [2020](http://nil.csail.mit.edu/6.824/2020/schedule.html) but I am doing the projects from [2026](https://pdos.csail.mit.edu/6.824/schedule.html), so I do all readings from 2020 and 2026 to prepare for each.

Rule of thumb for creating a superset of reading:
- If a topic's paper changes I read both
- Generally ordered to match the 2020 lecture videos with the 2026 exclusive readings slotted in near their 2026 position

| 2020 video | Topic                                 | Reading                               | Status |
|------------|---------------------------------------|---------------------------------------|--------|
| L1         | Introduction                          | MapReduce                             | Done   |
| L2         | RPC and threads                       | Go tour                               | Done   |
| L3         | GFS                                   | GFS                                   | Done   |
|            | Paxos (2026 only)                     | Paxos Made Simple                     | Done   |
| L4         | Primary-backup                        | VM-FT (2020 only)                     | Done   |
| L5         | Go                                    | Go memory model, 2026: Go paper       | Done   |
| L6         | Raft (1)                              | Raft §1 to 5                          | Done   |
| L7         | Raft (2)                              | Raft §7 to end                        | Done   |
|            | Linearizability (2026 only)           | Herlihy, through §3.1                 | Done   |
| L8         | ZooKeeper                             | ZooKeeper                             | Done   |
| L9         | Chain replication                     | CRAQ, 2026: CR                        |        |
| L10        | Aurora (2020 only)                    | Aurora                                |        |
| L11        | Frangipani (2020 only)                | Frangipani                            |        |
| L12        | Distributed transactions              | 6.033 Ch 9                            |        |
| L13        | Spanner                               | Spanner                               |        |
| L14        | Optimistic concurrency control        | FaRM                                  |        |
|            | Verification (2026 only)              | IronFleet                             |        |
| L15        | Spark (2020 only)                     | Spark                                 |        |
| L16        | Cache consistency                     | Memcached at Facebook                 |        |
|            | AWS Lambda (2026 only)                | On-demand container loading           |        |
|            | Ray (2026 only)                       | Ray                                   |        |
| L17        | Causal consistency (2020 only)        | COPS                                  |        |
| L18        | Fork consistency                      | Certificate Transparency, 2026: SUNDR |        |
| L19        | Bitcoin                               | Bitcoin                               |        |
|            | Byzantine fault tolerance (2026 only) | Practical BFT                         |        |
| L20        | Blockstack (2020 only)                | BlockStack                            |        |
