from Bio.Seq import Seq


seq = Seq("AGTACACTGGT")
print(seq.reverse_complement())
print(seq.complement())
print(seq.reverse_complement_rna())
