package org.squoia.orm.base;

public class MatchConst {

    //匹配符 	描述 	示例
    public static final String _GT = "$gt";            //大于 	db.foo.bar.find({age:{$gt:20}})
    public static final String _GTE = "$gte";            //大于等于 	db.foo.bar.find({age:{$gte:20}})
    public static final String _LT = "$lt";            //小于 	foo.bar.find({age:{$lt:20}})
    public static final String _LTE = "$lte";            //小于等于 	db.foo.bar.find({age:{$lte:20}})
    public static final String _NE = "$ne";            //不等于 	db.foo.bar.find({age:{$ne:20}})
    public static final String _ET = "$et";            //等于 	db.foo.bar.find({age:{$et:20}})
    public static final String _MOD = "$mod";            //取模 	db.foo.bar.find({age:{$mod:[6,5]}})
    public static final String _IN = "$in";            //集合内存在 	db.foo.bar.find({age:{$in:[20,21]}})
    public static final String _ISNULL = "$isnull";    //	为 null 或不存在 	Db.foo.bar.find({$age:{$isnull:1}})
    public static final String _NIN = "$nin";            //集合内不存在 	db.foo.bar.find({age:{$nin:[20,21]})
    public static final String _ALL = "$all";            //全部 	db.foo.bar.find({age:{$all:[20,21]}})
    public static final String _AND = "$and";            //与 	db.foo.bar.find({$and:{age:20},{name:"Tom"}})
    public static final String _NOT = "$not";            //非 	db.foo.bar.find({$not:{age:20},{name:"Tom"}})
    public static final String _OR = "$or";            //或 	db.foo.bar.find({$or:{age:20},{name:"Tom"}})
    public static final String _TYPE = "$type";        //数据类型 	db.foo.bar.find({age:{$type:16}})
    public static final String _EXISTS = "$exists";    //存在 	db.foo.bar.find({age:{$exists:1}})
    public static final String _ELEM_MATCH = "$elemMatch";       //元素匹配 	db.foo.bar.find({age:{$elemMatch:20}})
    //$+标识符 											        //$+标识符 数组元素匹配 	db.foo.bar.find({"array.$2":10})
    public static final String _SIZE = "$size";                //大小 	db.foo.bar.find({array:{$size:3}})
    public static final String _REGEX = "$regex";             //正则表达式 	db.foo.bar.find({str:{$regex:'dh.*fj',$options:'i'}})
    public static final String _OPTIONS = "$options";        //正则表达式 	db.foo.bar.find({str:{$regex:'dh.*fj',$options:'i'}})
    public static final String _I = "i";                    // 忽略大小写的表达
    public static final String _PUSH = "$push";
    public static final String _PULL = "$pull";
    public static final String _INC = "$inc";    //增加指定的数量
    public static final String _SET = "$set";


}
