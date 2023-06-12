import org.apache.commons.lang.ArrayUtils;

import java.util.Arrays;

/**
 * @Author XieChuangJian
 * @Date 2022/4/14
 */
public class testBinarySearch {

    public static void main(String[] args) {
        String[] frequentDeleteTables = "amall_goods.goods_shelf_list,amall_goods.goods_shelf_sku,amall_activity.strategy_wtlist_mbr,amall_welfare.welfare_shopping_cart,amall_activity.activity_goods_withdraw,amall_goods.goods_pack_list,amall_activity.activity_goods_cate".split(",");
//          frequentDeleteTables
//        ArrayList<String> arr=new ArrayList(Arrays.asList(frequentDeleteTables));
////        System.out.println("abc".compareTo("abd"));
//        arr.sort((x,y)->{return x.compareTo(y);});
//        Arrays.binarySearch(arr,"sad");
        long startTime = System.nanoTime();
        Arrays.sort(frequentDeleteTables);
        for (int i = 0; i < 1000000; i++) {
            binarySearch(frequentDeleteTables, "amall_activity.strategy_wtlist_mb");
        }
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        System.out.println("binarySearch:  " + duration / 1000000);

        startTime = System.nanoTime();
        Arrays.sort(frequentDeleteTables);
        for (int i = 0; i < 1000000; i++) {
            contains(frequentDeleteTables, "amall_activity.strategy_wtlist_mb");
        }
        endTime = System.nanoTime();
        duration = endTime - startTime;
        System.out.println("contains:  " + duration / 1000000);

        startTime = System.nanoTime();
        Arrays.sort(frequentDeleteTables);
        for (int i = 0; i < 1000000; i++) {
            contains(frequentDeleteTables, "amall_activity.strategy_wtlist_mb");
        }
        endTime = System.nanoTime();
        duration = endTime - startTime;
        System.out.println("loop:  " + duration / 1000000);

    }

    static boolean binarySearch(String[] frequentDeleteTables, String key) {
        if (Arrays.binarySearch(frequentDeleteTables, key) < 0) {
            return false;
        }
        return true;
    }

    static boolean contains(String[] frequentDeleteTables, String key) {
        return ArrayUtils.contains(frequentDeleteTables, key);
    }

    static boolean loop(String[] frequentDeleteTables, String key) {
        for (String frequentDeleteTable : frequentDeleteTables) {
            if (frequentDeleteTable.equals(key))
                return true;
        }
        return false;
    }

}
