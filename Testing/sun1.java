public class SampleJob {
  public static void main(String[] args) {
	int x;
	String str1 = "sun man dan chan gun";
	String[] list = str1.split(" ");
	System.out.println(list[1]);
	StringTokenizer itr = new StringTokenizer("sunil|sadsgd|fsdgs|fdgsd|sgd", "|");
	Scanner sc = new Scanner("ggg hhh iii kkk lll vvvv cccc ddd");
//	Scanner st = sc.useDelimiter(" ");
//	System.out.println(st.nextInt());
	List mylist = new ArrayList();
	mylist.add(1);
	mylist.add(2);
	mylist.add(4);
    System.out.println(mylist.get(0));
    x = itr.countTokens();
    while (itr.hasMoreTokens()) {
       System.out.println(itr.nextToken());
    }
  }
}
