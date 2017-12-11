package sha;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static sha.Utils.readJsonFromClasspath;

public class Test1
{
    private static final Logger log = LogManager.getLogger();
    private static Settings s;

    public static void main( String[] args ) {
        try {
            Test1 obj = new Test1();
            try {
                s = readJsonFromClasspath("settings.json", Settings.class);
            } catch (Exception e) {
//                log.warn("settings.json not found on classpath");
                s = new Settings();
            }
//            log.info("Using settings:{}", dumps(s));
            obj.go();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static class Settings {
        public String dummy = "";
        // required for jackson
        public Settings() {
        }
    }


    /**
     * All teh code from here:
     */
    private void go() throws Exception {
        int len = 1<<10;
        int mask = len - 1;
        Object[] a1 = new Object[len];
        int count = 0;
        int num = 1000_000_00;
        for(int i=0; i<a1.length; i++) {
            int x = Utils.random(0, 2);
            if(x==0) {
                a1[i]= new C1();
            } else {
                a1[i] = new C2();
            }
        }
        long now = System.nanoTime();
        for(int i=0; i<num; i++) {
            int ind = i&mask;
            if(a1[ind] instanceof C1) {
                count++;
            }
        }
        long dur = System.nanoTime() - now;

        dur /= num;

        log.debug("yoyo count:{} nanosPerOp:{}", count, dur);
    }

    interface Parent {
        void someMethod();
    }

    static class C1 implements Parent {

        @Override
        public void someMethod() {

        }
    }
    static class C2 implements Parent {

    @Override
    public void someMethod() {

    }
}



}
