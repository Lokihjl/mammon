package com.lokihjl.mammon.utils;
/**
 * @author hjl
 * @E-mail:huangjl@neunn.com
 * @version 创建时间：2016年4月8日 下午5:29:42
 */
public class StringUtils {
    
    public static boolean isEmpty(final String str) {
        if(null == str || str.trim().equals("")) {
            return true ;
        }
        return false ;
    }
    
    public static boolean isNoEmpty(final String str) {
        if(isEmpty(str)) {
            return false ;
        }
        
        return true ;
    }

}
