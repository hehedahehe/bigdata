package mapreduce.tools;


import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liruibo on 2017/8/9.
 */
public class MySort<T extends Comparable> {

    public  void quickSort(List<T> a){
        int lo = 0, hi = a.size()-1;
        quickSort(a,lo,hi);
    }

    private  void quickSort(List<T> a, int lo, int hi){
        if(hi<=lo){
            return;
        }
        int j = partion(a, lo, hi);
        quickSort(a,lo,j-1);
        quickSort(a,j+1,hi);
    }

    private  int partion(List<T> a, int lo, int hi){
        int i = lo, j = hi+1;
        T v = a.get(lo);
        while(true){
            while(!(a.get(i).compareTo(v)>0)){
                i++;
                if(i==hi)break;
            }
            while(!(a.get(--j).compareTo(v)<=0)){
                if(j==lo)break;
            }
            if(j<=i)break;
            exchange(a, i,j);
        }
        exchange(a, lo,j);
        return j;
    }


    private  void exchange(List<T> a, int i, int j){
        T tmp = a.get(i);
        a.set(i, a.get(j));
        a.set(j,tmp);
    }

    @Test
    public void testQuickSort(){
        List<Integer> list = new ArrayList<>();
//        list.add(1);
        list.add(3);
//        list.add(2);
        list.add(1);
        list.add(1);
        System.out.println(list.toString());
        new MySort<Integer>().quickSort(list);
        System.out.println(list.toString());
    }


}
