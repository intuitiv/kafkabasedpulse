package distributedmsg;

import java.io.Serializable;

public class PulseRecord implements Serializable {
	private static final long serialVersionUID = 1L;
	String rulename;
	EVENTTYPE event;

	static enum EVENTTYPE {
		CREATE, UPDATE, DELETE
	};

	public PulseRecord(String ruleName, EVENTTYPE event) {
		this.rulename = ruleName;
		this.event = event;
	}

	@Override
	public String toString() {
		return rulename + " of type " + event;
	}
}
