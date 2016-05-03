package io.github.bckfnn.logimport;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import io.github.bckfnn.mongodb.Callback;
import io.github.bckfnn.mongodb.Client;
import io.github.bckfnn.mongodb.Database;
import io.github.bckfnn.mongodb.WriteConcern;
import io.github.bckfnn.mongodb.bson.BsonDoc;
import io.github.bckfnn.mongodb.bson.BsonDocMap;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.parsetools.RecordParser;

public class LogImport {
    Vertx vertx;
    SimpleDateFormat df = new SimpleDateFormat("dd/MMM/YY:HH:mm:ss Z", Locale.ENGLISH);
    Client client;

    public static void main(String[] args) {
        LogImport li = new LogImport();
        li.run((c, e) -> {
            if (e != null) {
                e.printStackTrace();
            }
            System.out.println("done");
            li.vertx.close();
        });
    }

    public LogImport() {
        vertx = Vertx.vertx();
        client = new Client(vertx, "localhost", 27017);

    }

    public void run(Callback<Void> cb) {
        client.open(cb.handler(v -> {
            Database db = client.database("logimport");

            FileSystem fs = vertx.fileSystem();
            fs.readDir("d:/vertx/apache-logs", ar -> {
                if (ar.failed()) {
                    throw new RuntimeException(ar.cause());
                }

                forEach(ar.result(), (s, h) -> {
                    if (s.contains("logfile.") && s.endsWith(".log")) {
                        load(s, db, h);
                    } else {
                        h.ok();
                    }
                }, cb);
            });
        }));

    }

    private void load(String path, Database db, Callback<Void> cb) {
        List<BsonDoc> docs = new ArrayList<>();

        System.out.println(path);
        AtomicBoolean paused = new AtomicBoolean(false);
        vertx.fileSystem().open(path, new OpenOptions().setRead(true).setWrite(false), cb.handler(file -> {
            System.out.println("opened");
            file.endHandler($ -> {
                System.out.println("endhandler");
                file.close(cb.handler($2 -> {
                    System.out.println("closed");
                    cb.ok();
                }));
            });
            file.exceptionHandler(e -> {
                cb.fail(e);
            });
            file.handler(RecordParser.newDelimited("\n", b -> {
                /*
                if (!paused.get()) {
                    file.pause();
                    paused.set(true);
                    System.out.println("pause");
                } 
                 */                   
                try {
                    BsonDoc doc = parseLine(b.toString());
                    docs.add(doc);

                    if (docs.size() > 2000) {
                        //System.out.println(doc);
                        db.collection("logs"). insert(docs, WriteConcern.NONE, x -> {
                            //System.out.println(wr);
                            /*
                        if (wr.getOk() != 0) {
                            if (paused.get()) {
                                System.out.println("resume");
                                file.resume();    
                                paused.set(false);
                            }

                        } else {
                            System.err.println(wr);
                        }
                             */
                        });
                        docs.clear();
                    }
                } catch (Exception e1) {
                    cb.fail(e1);
                    //file.pause();
                }
            }));
        }));
    }

    public static <T> void forEach(List<T> list, BiConsumer<T, Callback<Void>> x, Callback<Void> handler) {
        AtomicInteger cnt = new AtomicInteger(0);
        Callback<Void> h = new Callback<Void>() {
            @Override
            public void call(Void $, Throwable e) {
                if (e != null) {
                    System.out.println("forEach stopped");
                    handler.fail(e);
                    return;
                }
                int i = cnt.getAndIncrement();
                if (i >= list.size()) {
                    //log.trace("forEach.done {} items", list.size());
                    handler.ok();
                } else {
                    x.accept(list.get(i), this);
                }
            }
        };
        h.call(null, null);

    }

    BsonDoc parseLine(String line) throws ParseException  {
        Pos pos = new Pos(line);
        String ip = pos.tokenTo(' ');
        String user = pos.tokenTo(' ');
        @SuppressWarnings("unused")
        String xxx = pos.tokenTo(' ');
        pos.tokenTo('[');
        String date = pos.tokenTo(']');
        pos.tokenTo('"');

        String proto;
        String url;
        String cmd = pos.tokenTo(' ');
        if (cmd != null && cmd.startsWith("-")) {
            cmd = proto = url = "-";
        } else {
            url = pos.tokenTo(' ');
            proto = pos.tokenTo('"');
            pos.tokenTo(' ');
        }

        String code = pos.tokenTo(' ');
        String size = pos.tokenTo(' ');
        @SuppressWarnings("unused")
        String zzz = pos.tokenTo(' ');

        BsonDoc o = new BsonDocMap();
        o.put("ip", ip);
        o.put("user", user);
        o.put("date", df.parse(date));
        o.put("cmd", cmd);
        o.put("url", url);
        if (proto != null) {
            o.put("proto", proto);
        }
        if (code != null) {
            o.put("code", code);
        }
        if (size != null && !size.equals("-")) {
            o.put("size", Long.parseLong(size));
        }
        return o;
    }


    static class Pos {
        int pos = 0;
        String line;

        Pos(String line) {
            this.line = line;
        }

        String tokenTo(char delim) {
            int idx = line.indexOf(delim, pos);
            if (idx < 0) {
                return null;
            }
            String r = line.substring(pos, idx);
            pos = idx + 1;
            return r;
        }

        void advance() {
            pos++;
        }
    }

}