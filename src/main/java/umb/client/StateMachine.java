package umb.client;

public  class StateMachine {

    public static final StateMachine obj = new StateMachine();
    public Object go(Event e, Object ... args) {
        return null;
    }

    enum Event {
        a1;
    }
}
