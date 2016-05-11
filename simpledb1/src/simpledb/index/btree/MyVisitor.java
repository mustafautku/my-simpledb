package simpledb.index.btree;

class MyVisitor 
{
	public int m_indexIO = 0;
	public int m_leafIO = 0;

	public void visitIndex()
	{
		m_indexIO++;
	}

	public void visitLeaf()
	{
		m_leafIO++;
	}
}
