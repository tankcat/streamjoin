package util.mfm;

import tool.SystemParameters;

import java.io.Serializable;

public class Matrix implements Serializable{
	private static final long serialVersionUID = -8345924916894132832L;
	public int row;
	public int col;
	public boolean isUpRight;
	public int nodeCount;
	public int discardCount;

	/*
	 * para:行数
	 *      列数
	 *      节点的总数
	 *      删除的数量
	 *      行上缺失，或者列上缺失
	 */
	public Matrix(int row,int col,int nodeCount,int discardCount,int isUpRight){
		this.row=row;
		this.col=col;
		this.isUpRight=(isUpRight==1?true:false);
		this.nodeCount=nodeCount;
		this.discardCount=discardCount;
	}
	
	public Matrix(int[] attr){
		this.row=attr[0];
		this.col=attr[1];
		this.isUpRight=(attr[4]==1?true:false);
		this.nodeCount=attr[2];
		this.discardCount=attr[3];
		System.out.print("新的矩阵："+row+"行，"+col+"列，共"+nodeCount+"个节点，删除");
		if(isUpRight==true){
			System.out.println("最后一列"+discardCount+"个节点");
		}else{
			System.out.println("最后一行"+discardCount+"个节点");
		}
	}
	
	public Matrix(int row,int col){
		this.row=row;
		this.col=col;
	}
	
	public String toString(){
		return "行＝"+row+",列＝"+col+",丢弃＝"+discardCount+",节点个数＝"+nodeCount;
	}
}