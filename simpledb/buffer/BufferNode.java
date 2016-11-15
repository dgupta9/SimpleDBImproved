package simpledb.buffer;

public class BufferNode {
	private Buffer val;
	private BufferNode next;
	
	public BufferNode(Buffer val, BufferNode next) {
		super();
		this.val = val;
		this.next = next;
	}
	
	public BufferNode(Buffer val) {
		super();
		this.val = val;
		this.next = null;
	}

	public Buffer getVal() {
		return val;
	}

	public void setVal(Buffer val) {
		this.val = val;
	}

	public BufferNode getNext() {
		return next;
	}

	public void setNext(BufferNode next) {
		this.next = next;
	}
	
	
}
