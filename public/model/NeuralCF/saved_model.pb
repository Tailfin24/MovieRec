��
�"�"
^
AssignVariableOp
resource
value"dtype"
dtypetype"
validate_shapebool( �
p
BatchMatMulV2
x"T
y"T
output"T"
Ttype:
2	"
adj_xbool( "
adj_ybool( 
�
BiasAdd

value"T	
bias"T
output"T""
Ttype:
2	"-
data_formatstringNHWC:
NHWCNCHW
N
Cast	
x"SrcT	
y"DstT"
SrcTtype"
DstTtype"
Truncatebool( 
h
ConcatV2
values"T*N
axis"Tidx
output"T"
Nint(0"	
Ttype"
Tidxtype0:
2	
8
Const
output"dtype"
valuetensor"
dtypetype
$
DisableCopyOnRead
resource�
W

ExpandDims

input"T
dim"Tdim
output"T"	
Ttype"
Tdimtype0:
2	
q
GatherNd
params"Tparams
indices"Tindices
output"Tparams"
Tparamstype"
Tindicestype:
2	
�
GatherV2
params"Tparams
indices"Tindices
axis"Taxis
output"Tparams"

batch_dimsint "
Tparamstype"
Tindicestype:
2	"
Taxistype:
2	
B
GreaterEqual
x"T
y"T
z
"
Ttype:
2	
.
Identity

input"T
output"T"	
Ttype
u
MatMul
a"T
b"T
product"T"
transpose_abool( "
transpose_bbool( "
Ttype:
2	
�
MergeV2Checkpoints
checkpoint_prefixes
destination_prefix"
delete_old_dirsbool("
allow_missing_filesbool( �

NoOp
U
NotEqual
x"T
y"T
z
"	
Ttype"$
incompatible_shape_errorbool(�
M
Pack
values"T*N
output"T"
Nint(0"	
Ttype"
axisint 
C
Placeholder
output"dtype"
dtypetype"
shapeshape:
�
Prod

input"T
reduction_indices"Tidx
output"T"
	keep_dimsbool( ""
Ttype:
2	"
Tidxtype0:
2	
@
ReadVariableOp
resource
value"dtype"
dtypetype�
E
Relu
features"T
activations"T"
Ttype:
2	
[
Reshape
tensor"T
shape"Tshape
output"T"	
Ttype"
Tshapetype0:
2	
�
ResourceGather
resource
indices"Tindices
output"dtype"

batch_dimsint "
validate_indicesbool("
dtypetype"
Tindicestype:
2	�
o
	RestoreV2

prefix
tensor_names
shape_and_slices
tensors2dtypes"
dtypes
list(type)(0�
l
SaveV2

prefix
tensor_names
shape_and_slices
tensors2dtypes"
dtypes
list(type)(0�
?
Select
	condition

t"T
e"T
output"T"	
Ttype
d
Shape

input"T&
output"out_type��out_type"	
Ttype"
out_typetype0:
2	
H
ShardedFilename
basename	
shard

num_shards
filename
0
Sigmoid
x"T
y"T"
Ttype:

2
a
Slice

input"T
begin"Index
size"Index
output"T"	
Ttype"
Indextype:
2	
�
SparseFillEmptyRows
indices	
values"T
dense_shape	
default_value"T
output_indices	
output_values"T
empty_row_indicator

reverse_index_map	"	
Ttype
h
SparseReshape
input_indices	
input_shape	
	new_shape	
output_indices	
output_shape	
�
SparseSegmentMean	
data"T
indices"Tidx
segment_ids"Tsegmentids
output"T"
Ttype:
2"
Tidxtype0:
2	"
Tsegmentidstype0:
2	
N
Squeeze

input"T
output"T"	
Ttype"
squeeze_dims	list(int)
 (
�
StatefulPartitionedCall
args2Tin
output2Tout"
Tin
list(type)("
Tout
list(type)("	
ffunc"
configstring "
config_protostring "
executor_typestring ��
@
StaticRegexFullMatch	
input

output
"
patternstring
�
StridedSlice

input"T
begin"Index
end"Index
strides"Index
output"T"	
Ttype"
Indextype:
2	"

begin_maskint "
end_maskint "
ellipsis_maskint "
new_axis_maskint "
shrink_axis_maskint 
L

StringJoin
inputs*N

output"

Nint("
	separatorstring 
c
Tile

input"T
	multiples"
Tmultiples
output"T"	
Ttype"

Tmultiplestype0:
2	
P
Unique
x"T
y"T
idx"out_idx"	
Ttype"
out_idxtype0:
2	
�
VarHandleOp
resource"
	containerstring "
shared_namestring "
dtypetype"
shapeshape"#
allowed_deviceslist(string)
 �
G
Where

input"T	
index	"'
Ttype0
:
2	

&
	ZerosLike
x"T
y"T"	
Ttype"serve*2.13.12v2.13.0-17-gf841394b1b78��
w
false_negativesVarHandleOp*
_output_shapes
: *
dtype0*
shape:�* 
shared_namefalse_negatives
p
#false_negatives/Read/ReadVariableOpReadVariableOpfalse_negatives*
_output_shapes	
:�*
dtype0
w
false_positivesVarHandleOp*
_output_shapes
: *
dtype0*
shape:�* 
shared_namefalse_positives
p
#false_positives/Read/ReadVariableOpReadVariableOpfalse_positives*
_output_shapes	
:�*
dtype0
u
true_negativesVarHandleOp*
_output_shapes
: *
dtype0*
shape:�*
shared_nametrue_negatives
n
"true_negatives/Read/ReadVariableOpReadVariableOptrue_negatives*
_output_shapes	
:�*
dtype0
u
true_positivesVarHandleOp*
_output_shapes
: *
dtype0*
shape:�*
shared_nametrue_positives
n
"true_positives/Read/ReadVariableOpReadVariableOptrue_positives*
_output_shapes	
:�*
dtype0
{
false_negatives_1VarHandleOp*
_output_shapes
: *
dtype0*
shape:�*"
shared_namefalse_negatives_1
t
%false_negatives_1/Read/ReadVariableOpReadVariableOpfalse_negatives_1*
_output_shapes	
:�*
dtype0
{
false_positives_1VarHandleOp*
_output_shapes
: *
dtype0*
shape:�*"
shared_namefalse_positives_1
t
%false_positives_1/Read/ReadVariableOpReadVariableOpfalse_positives_1*
_output_shapes	
:�*
dtype0
y
true_negatives_1VarHandleOp*
_output_shapes
: *
dtype0*
shape:�*!
shared_nametrue_negatives_1
r
$true_negatives_1/Read/ReadVariableOpReadVariableOptrue_negatives_1*
_output_shapes	
:�*
dtype0
y
true_positives_1VarHandleOp*
_output_shapes
: *
dtype0*
shape:�*!
shared_nametrue_positives_1
r
$true_positives_1/Read/ReadVariableOpReadVariableOptrue_positives_1*
_output_shapes	
:�*
dtype0
^
countVarHandleOp*
_output_shapes
: *
dtype0*
shape: *
shared_namecount
W
count/Read/ReadVariableOpReadVariableOpcount*
_output_shapes
: *
dtype0
^
totalVarHandleOp*
_output_shapes
: *
dtype0*
shape: *
shared_nametotal
W
total/Read/ReadVariableOpReadVariableOptotal*
_output_shapes
: *
dtype0
b
count_1VarHandleOp*
_output_shapes
: *
dtype0*
shape: *
shared_name	count_1
[
count_1/Read/ReadVariableOpReadVariableOpcount_1*
_output_shapes
: *
dtype0
b
total_1VarHandleOp*
_output_shapes
: *
dtype0*
shape: *
shared_name	total_1
[
total_1/Read/ReadVariableOpReadVariableOptotal_1*
_output_shapes
: *
dtype0
~
Adam/v/dense_6/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:*$
shared_nameAdam/v/dense_6/bias
w
'Adam/v/dense_6/bias/Read/ReadVariableOpReadVariableOpAdam/v/dense_6/bias*
_output_shapes
:*
dtype0
~
Adam/m/dense_6/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:*$
shared_nameAdam/m/dense_6/bias
w
'Adam/m/dense_6/bias/Read/ReadVariableOpReadVariableOpAdam/m/dense_6/bias*
_output_shapes
:*
dtype0
�
Adam/v/dense_6/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
:*&
shared_nameAdam/v/dense_6/kernel

)Adam/v/dense_6/kernel/Read/ReadVariableOpReadVariableOpAdam/v/dense_6/kernel*
_output_shapes

:*
dtype0
�
Adam/m/dense_6/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
:*&
shared_nameAdam/m/dense_6/kernel

)Adam/m/dense_6/kernel/Read/ReadVariableOpReadVariableOpAdam/m/dense_6/kernel*
_output_shapes

:*
dtype0

Adam/v/dense_5/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:�*$
shared_nameAdam/v/dense_5/bias
x
'Adam/v/dense_5/bias/Read/ReadVariableOpReadVariableOpAdam/v/dense_5/bias*
_output_shapes	
:�*
dtype0

Adam/m/dense_5/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:�*$
shared_nameAdam/m/dense_5/bias
x
'Adam/m/dense_5/bias/Read/ReadVariableOpReadVariableOpAdam/m/dense_5/bias*
_output_shapes	
:�*
dtype0
�
Adam/v/dense_5/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape:	@�*&
shared_nameAdam/v/dense_5/kernel
�
)Adam/v/dense_5/kernel/Read/ReadVariableOpReadVariableOpAdam/v/dense_5/kernel*
_output_shapes
:	@�*
dtype0
�
Adam/m/dense_5/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape:	@�*&
shared_nameAdam/m/dense_5/kernel
�
)Adam/m/dense_5/kernel/Read/ReadVariableOpReadVariableOpAdam/m/dense_5/kernel*
_output_shapes
:	@�*
dtype0

Adam/v/dense_4/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:�*$
shared_nameAdam/v/dense_4/bias
x
'Adam/v/dense_4/bias/Read/ReadVariableOpReadVariableOpAdam/v/dense_4/bias*
_output_shapes	
:�*
dtype0

Adam/m/dense_4/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:�*$
shared_nameAdam/m/dense_4/bias
x
'Adam/m/dense_4/bias/Read/ReadVariableOpReadVariableOpAdam/m/dense_4/bias*
_output_shapes	
:�*
dtype0
�
Adam/v/dense_4/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape:	@�*&
shared_nameAdam/v/dense_4/kernel
�
)Adam/v/dense_4/kernel/Read/ReadVariableOpReadVariableOpAdam/v/dense_4/kernel*
_output_shapes
:	@�*
dtype0
�
Adam/m/dense_4/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape:	@�*&
shared_nameAdam/m/dense_4/kernel
�
)Adam/m/dense_4/kernel/Read/ReadVariableOpReadVariableOpAdam/m/dense_4/kernel*
_output_shapes
:	@�*
dtype0
~
Adam/v/dense_3/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:@*$
shared_nameAdam/v/dense_3/bias
w
'Adam/v/dense_3/bias/Read/ReadVariableOpReadVariableOpAdam/v/dense_3/bias*
_output_shapes
:@*
dtype0
~
Adam/m/dense_3/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:@*$
shared_nameAdam/m/dense_3/bias
w
'Adam/m/dense_3/bias/Read/ReadVariableOpReadVariableOpAdam/m/dense_3/bias*
_output_shapes
:@*
dtype0
�
Adam/v/dense_3/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
: @*&
shared_nameAdam/v/dense_3/kernel

)Adam/v/dense_3/kernel/Read/ReadVariableOpReadVariableOpAdam/v/dense_3/kernel*
_output_shapes

: @*
dtype0
�
Adam/m/dense_3/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
: @*&
shared_nameAdam/m/dense_3/kernel

)Adam/m/dense_3/kernel/Read/ReadVariableOpReadVariableOpAdam/m/dense_3/kernel*
_output_shapes

: @*
dtype0
~
Adam/v/dense_2/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:@*$
shared_nameAdam/v/dense_2/bias
w
'Adam/v/dense_2/bias/Read/ReadVariableOpReadVariableOpAdam/v/dense_2/bias*
_output_shapes
:@*
dtype0
~
Adam/m/dense_2/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:@*$
shared_nameAdam/m/dense_2/bias
w
'Adam/m/dense_2/bias/Read/ReadVariableOpReadVariableOpAdam/m/dense_2/bias*
_output_shapes
:@*
dtype0
�
Adam/v/dense_2/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
: @*&
shared_nameAdam/v/dense_2/kernel

)Adam/v/dense_2/kernel/Read/ReadVariableOpReadVariableOpAdam/v/dense_2/kernel*
_output_shapes

: @*
dtype0
�
Adam/m/dense_2/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
: @*&
shared_nameAdam/m/dense_2/kernel

)Adam/m/dense_2/kernel/Read/ReadVariableOpReadVariableOpAdam/m/dense_2/kernel*
_output_shapes

: @*
dtype0
~
Adam/v/dense_1/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape: *$
shared_nameAdam/v/dense_1/bias
w
'Adam/v/dense_1/bias/Read/ReadVariableOpReadVariableOpAdam/v/dense_1/bias*
_output_shapes
: *
dtype0
~
Adam/m/dense_1/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape: *$
shared_nameAdam/m/dense_1/bias
w
'Adam/m/dense_1/bias/Read/ReadVariableOpReadVariableOpAdam/m/dense_1/bias*
_output_shapes
: *
dtype0
�
Adam/v/dense_1/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
:
 *&
shared_nameAdam/v/dense_1/kernel

)Adam/v/dense_1/kernel/Read/ReadVariableOpReadVariableOpAdam/v/dense_1/kernel*
_output_shapes

:
 *
dtype0
�
Adam/m/dense_1/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
:
 *&
shared_nameAdam/m/dense_1/kernel

)Adam/m/dense_1/kernel/Read/ReadVariableOpReadVariableOpAdam/m/dense_1/kernel*
_output_shapes

:
 *
dtype0
z
Adam/v/dense/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape: *"
shared_nameAdam/v/dense/bias
s
%Adam/v/dense/bias/Read/ReadVariableOpReadVariableOpAdam/v/dense/bias*
_output_shapes
: *
dtype0
z
Adam/m/dense/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape: *"
shared_nameAdam/m/dense/bias
s
%Adam/m/dense/bias/Read/ReadVariableOpReadVariableOpAdam/m/dense/bias*
_output_shapes
: *
dtype0
�
Adam/v/dense/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
:
 *$
shared_nameAdam/v/dense/kernel
{
'Adam/v/dense/kernel/Read/ReadVariableOpReadVariableOpAdam/v/dense/kernel*
_output_shapes

:
 *
dtype0
�
Adam/m/dense/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
:
 *$
shared_nameAdam/m/dense/kernel
{
'Adam/m/dense/kernel/Read/ReadVariableOpReadVariableOpAdam/m/dense/kernel*
_output_shapes

:
 *
dtype0
�
:Adam/v/dense_features_1/userId_embedding/embedding_weightsVarHandleOp*
_output_shapes
: *
dtype0*
shape:
��
*K
shared_name<:Adam/v/dense_features_1/userId_embedding/embedding_weights
�
NAdam/v/dense_features_1/userId_embedding/embedding_weights/Read/ReadVariableOpReadVariableOp:Adam/v/dense_features_1/userId_embedding/embedding_weights* 
_output_shapes
:
��
*
dtype0
�
:Adam/m/dense_features_1/userId_embedding/embedding_weightsVarHandleOp*
_output_shapes
: *
dtype0*
shape:
��
*K
shared_name<:Adam/m/dense_features_1/userId_embedding/embedding_weights
�
NAdam/m/dense_features_1/userId_embedding/embedding_weights/Read/ReadVariableOpReadVariableOp:Adam/m/dense_features_1/userId_embedding/embedding_weights* 
_output_shapes
:
��
*
dtype0
�
9Adam/v/dense_features/movieId_embedding/embedding_weightsVarHandleOp*
_output_shapes
: *
dtype0*
shape:
��
*J
shared_name;9Adam/v/dense_features/movieId_embedding/embedding_weights
�
MAdam/v/dense_features/movieId_embedding/embedding_weights/Read/ReadVariableOpReadVariableOp9Adam/v/dense_features/movieId_embedding/embedding_weights* 
_output_shapes
:
��
*
dtype0
�
9Adam/m/dense_features/movieId_embedding/embedding_weightsVarHandleOp*
_output_shapes
: *
dtype0*
shape:
��
*J
shared_name;9Adam/m/dense_features/movieId_embedding/embedding_weights
�
MAdam/m/dense_features/movieId_embedding/embedding_weights/Read/ReadVariableOpReadVariableOp9Adam/m/dense_features/movieId_embedding/embedding_weights* 
_output_shapes
:
��
*
dtype0
n
learning_rateVarHandleOp*
_output_shapes
: *
dtype0*
shape: *
shared_namelearning_rate
g
!learning_rate/Read/ReadVariableOpReadVariableOplearning_rate*
_output_shapes
: *
dtype0
f
	iterationVarHandleOp*
_output_shapes
: *
dtype0	*
shape: *
shared_name	iteration
_
iteration/Read/ReadVariableOpReadVariableOp	iteration*
_output_shapes
: *
dtype0	
p
dense_6/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:*
shared_namedense_6/bias
i
 dense_6/bias/Read/ReadVariableOpReadVariableOpdense_6/bias*
_output_shapes
:*
dtype0
x
dense_6/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
:*
shared_namedense_6/kernel
q
"dense_6/kernel/Read/ReadVariableOpReadVariableOpdense_6/kernel*
_output_shapes

:*
dtype0
q
dense_5/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:�*
shared_namedense_5/bias
j
 dense_5/bias/Read/ReadVariableOpReadVariableOpdense_5/bias*
_output_shapes	
:�*
dtype0
y
dense_5/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape:	@�*
shared_namedense_5/kernel
r
"dense_5/kernel/Read/ReadVariableOpReadVariableOpdense_5/kernel*
_output_shapes
:	@�*
dtype0
q
dense_4/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:�*
shared_namedense_4/bias
j
 dense_4/bias/Read/ReadVariableOpReadVariableOpdense_4/bias*
_output_shapes	
:�*
dtype0
y
dense_4/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape:	@�*
shared_namedense_4/kernel
r
"dense_4/kernel/Read/ReadVariableOpReadVariableOpdense_4/kernel*
_output_shapes
:	@�*
dtype0
p
dense_3/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:@*
shared_namedense_3/bias
i
 dense_3/bias/Read/ReadVariableOpReadVariableOpdense_3/bias*
_output_shapes
:@*
dtype0
x
dense_3/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
: @*
shared_namedense_3/kernel
q
"dense_3/kernel/Read/ReadVariableOpReadVariableOpdense_3/kernel*
_output_shapes

: @*
dtype0
p
dense_2/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape:@*
shared_namedense_2/bias
i
 dense_2/bias/Read/ReadVariableOpReadVariableOpdense_2/bias*
_output_shapes
:@*
dtype0
x
dense_2/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
: @*
shared_namedense_2/kernel
q
"dense_2/kernel/Read/ReadVariableOpReadVariableOpdense_2/kernel*
_output_shapes

: @*
dtype0
p
dense_1/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape: *
shared_namedense_1/bias
i
 dense_1/bias/Read/ReadVariableOpReadVariableOpdense_1/bias*
_output_shapes
: *
dtype0
x
dense_1/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
:
 *
shared_namedense_1/kernel
q
"dense_1/kernel/Read/ReadVariableOpReadVariableOpdense_1/kernel*
_output_shapes

:
 *
dtype0
l

dense/biasVarHandleOp*
_output_shapes
: *
dtype0*
shape: *
shared_name
dense/bias
e
dense/bias/Read/ReadVariableOpReadVariableOp
dense/bias*
_output_shapes
: *
dtype0
t
dense/kernelVarHandleOp*
_output_shapes
: *
dtype0*
shape
:
 *
shared_namedense/kernel
m
 dense/kernel/Read/ReadVariableOpReadVariableOpdense/kernel*
_output_shapes

:
 *
dtype0
�
3dense_features_1/userId_embedding/embedding_weightsVarHandleOp*
_output_shapes
: *
dtype0*
shape:
��
*D
shared_name53dense_features_1/userId_embedding/embedding_weights
�
Gdense_features_1/userId_embedding/embedding_weights/Read/ReadVariableOpReadVariableOp3dense_features_1/userId_embedding/embedding_weights* 
_output_shapes
:
��
*
dtype0
�
2dense_features/movieId_embedding/embedding_weightsVarHandleOp*
_output_shapes
: *
dtype0*
shape:
��
*C
shared_name42dense_features/movieId_embedding/embedding_weights
�
Fdense_features/movieId_embedding/embedding_weights/Read/ReadVariableOpReadVariableOp2dense_features/movieId_embedding/embedding_weights* 
_output_shapes
:
��
*
dtype0
r
serving_default_movieIdPlaceholder*#
_output_shapes
:���������*
dtype0*
shape:���������
q
serving_default_userIdPlaceholder*#
_output_shapes
:���������*
dtype0*
shape:���������
�
StatefulPartitionedCallStatefulPartitionedCallserving_default_movieIdserving_default_userId3dense_features_1/userId_embedding/embedding_weights2dense_features/movieId_embedding/embedding_weightsdense_1/kerneldense_1/biasdense/kernel
dense/biasdense_3/kerneldense_3/biasdense_2/kerneldense_2/biasdense_4/kerneldense_4/biasdense_5/kerneldense_5/biasdense_6/kerneldense_6/bias*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������*2
_read_only_resource_inputs
	
*-
config_proto

CPU

GPU 2J 8� *-
f(R&
$__inference_signature_wrapper_403273

NoOpNoOp
�s
ConstConst"/device:CPU:0*
_output_shapes
: *
dtype0*�s
value�sB�s B�r
�
layer-0
layer-1
layer_with_weights-0
layer-2
layer_with_weights-1
layer-3
layer_with_weights-2
layer-4
layer_with_weights-3
layer-5
layer_with_weights-4
layer-6
layer_with_weights-5
layer-7
	layer_with_weights-6
	layer-8

layer_with_weights-7

layer-9
layer-10
layer_with_weights-8
layer-11
	variables
trainable_variables
regularization_losses
	keras_api
__call__
*&call_and_return_all_conditional_losses
_default_save_signature
	optimizer

signatures*
* 
* 
�
	variables
trainable_variables
regularization_losses
	keras_api
__call__
*&call_and_return_all_conditional_losses
_feature_columns

_resources
'#movieId_embedding/embedding_weights*
�
	variables
 trainable_variables
!regularization_losses
"	keras_api
#__call__
*$&call_and_return_all_conditional_losses
%_feature_columns
&
_resources
&'"userId_embedding/embedding_weights*
�
(	variables
)trainable_variables
*regularization_losses
+	keras_api
,__call__
*-&call_and_return_all_conditional_losses

.kernel
/bias*
�
0	variables
1trainable_variables
2regularization_losses
3	keras_api
4__call__
*5&call_and_return_all_conditional_losses

6kernel
7bias*
�
8	variables
9trainable_variables
:regularization_losses
;	keras_api
<__call__
*=&call_and_return_all_conditional_losses

>kernel
?bias*
�
@	variables
Atrainable_variables
Bregularization_losses
C	keras_api
D__call__
*E&call_and_return_all_conditional_losses

Fkernel
Gbias*
�
H	variables
Itrainable_variables
Jregularization_losses
K	keras_api
L__call__
*M&call_and_return_all_conditional_losses

Nkernel
Obias*
�
P	variables
Qtrainable_variables
Rregularization_losses
S	keras_api
T__call__
*U&call_and_return_all_conditional_losses

Vkernel
Wbias*
�
X	variables
Ytrainable_variables
Zregularization_losses
[	keras_api
\__call__
*]&call_and_return_all_conditional_losses* 
�
^	variables
_trainable_variables
`regularization_losses
a	keras_api
b__call__
*c&call_and_return_all_conditional_losses

dkernel
ebias*
z
0
'1
.2
/3
64
75
>6
?7
F8
G9
N10
O11
V12
W13
d14
e15*
z
0
'1
.2
/3
64
75
>6
?7
F8
G9
N10
O11
V12
W13
d14
e15*
* 
�
fnon_trainable_variables

glayers
hmetrics
ilayer_regularization_losses
jlayer_metrics
	variables
trainable_variables
regularization_losses
__call__
_default_save_signature
*&call_and_return_all_conditional_losses
&"call_and_return_conditional_losses*

ktrace_0
ltrace_1* 

mtrace_0
ntrace_1* 
* 
�
o
_variables
p_iterations
q_learning_rate
r_index_dict
s
_momentums
t_velocities
u_update_step_xla*

vserving_default* 

0*

0*
* 
�
wnon_trainable_variables

xlayers
ymetrics
zlayer_regularization_losses
{layer_metrics
	variables
trainable_variables
regularization_losses
__call__
*&call_and_return_all_conditional_losses
&"call_and_return_conditional_losses*

|trace_0
}trace_1* 

~trace_0
trace_1* 
* 
* 
��
VARIABLE_VALUE2dense_features/movieId_embedding/embedding_weightsTlayer_with_weights-0/movieId_embedding.Sembedding_weights/.ATTRIBUTES/VARIABLE_VALUE*

'0*

'0*
* 
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
	variables
 trainable_variables
!regularization_losses
#__call__
*$&call_and_return_all_conditional_losses
&$"call_and_return_conditional_losses*

�trace_0
�trace_1* 

�trace_0
�trace_1* 
* 
* 
��
VARIABLE_VALUE3dense_features_1/userId_embedding/embedding_weightsSlayer_with_weights-1/userId_embedding.Sembedding_weights/.ATTRIBUTES/VARIABLE_VALUE*

.0
/1*

.0
/1*
* 
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
(	variables
)trainable_variables
*regularization_losses
,__call__
*-&call_and_return_all_conditional_losses
&-"call_and_return_conditional_losses*

�trace_0* 

�trace_0* 
\V
VARIABLE_VALUEdense/kernel6layer_with_weights-2/kernel/.ATTRIBUTES/VARIABLE_VALUE*
XR
VARIABLE_VALUE
dense/bias4layer_with_weights-2/bias/.ATTRIBUTES/VARIABLE_VALUE*

60
71*

60
71*
* 
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
0	variables
1trainable_variables
2regularization_losses
4__call__
*5&call_and_return_all_conditional_losses
&5"call_and_return_conditional_losses*

�trace_0* 

�trace_0* 
^X
VARIABLE_VALUEdense_1/kernel6layer_with_weights-3/kernel/.ATTRIBUTES/VARIABLE_VALUE*
ZT
VARIABLE_VALUEdense_1/bias4layer_with_weights-3/bias/.ATTRIBUTES/VARIABLE_VALUE*

>0
?1*

>0
?1*
* 
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
8	variables
9trainable_variables
:regularization_losses
<__call__
*=&call_and_return_all_conditional_losses
&="call_and_return_conditional_losses*

�trace_0* 

�trace_0* 
^X
VARIABLE_VALUEdense_2/kernel6layer_with_weights-4/kernel/.ATTRIBUTES/VARIABLE_VALUE*
ZT
VARIABLE_VALUEdense_2/bias4layer_with_weights-4/bias/.ATTRIBUTES/VARIABLE_VALUE*

F0
G1*

F0
G1*
* 
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
@	variables
Atrainable_variables
Bregularization_losses
D__call__
*E&call_and_return_all_conditional_losses
&E"call_and_return_conditional_losses*

�trace_0* 

�trace_0* 
^X
VARIABLE_VALUEdense_3/kernel6layer_with_weights-5/kernel/.ATTRIBUTES/VARIABLE_VALUE*
ZT
VARIABLE_VALUEdense_3/bias4layer_with_weights-5/bias/.ATTRIBUTES/VARIABLE_VALUE*

N0
O1*

N0
O1*
* 
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
H	variables
Itrainable_variables
Jregularization_losses
L__call__
*M&call_and_return_all_conditional_losses
&M"call_and_return_conditional_losses*

�trace_0* 

�trace_0* 
^X
VARIABLE_VALUEdense_4/kernel6layer_with_weights-6/kernel/.ATTRIBUTES/VARIABLE_VALUE*
ZT
VARIABLE_VALUEdense_4/bias4layer_with_weights-6/bias/.ATTRIBUTES/VARIABLE_VALUE*

V0
W1*

V0
W1*
* 
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
P	variables
Qtrainable_variables
Rregularization_losses
T__call__
*U&call_and_return_all_conditional_losses
&U"call_and_return_conditional_losses*

�trace_0* 

�trace_0* 
^X
VARIABLE_VALUEdense_5/kernel6layer_with_weights-7/kernel/.ATTRIBUTES/VARIABLE_VALUE*
ZT
VARIABLE_VALUEdense_5/bias4layer_with_weights-7/bias/.ATTRIBUTES/VARIABLE_VALUE*
* 
* 
* 
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
X	variables
Ytrainable_variables
Zregularization_losses
\__call__
*]&call_and_return_all_conditional_losses
&]"call_and_return_conditional_losses* 

�trace_0* 

�trace_0* 

d0
e1*

d0
e1*
* 
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
^	variables
_trainable_variables
`regularization_losses
b__call__
*c&call_and_return_all_conditional_losses
&c"call_and_return_conditional_losses*

�trace_0* 

�trace_0* 
^X
VARIABLE_VALUEdense_6/kernel6layer_with_weights-8/kernel/.ATTRIBUTES/VARIABLE_VALUE*
ZT
VARIABLE_VALUEdense_6/bias4layer_with_weights-8/bias/.ATTRIBUTES/VARIABLE_VALUE*
* 
Z
0
1
2
3
4
5
6
7
	8

9
10
11*
$
�0
�1
�2
�3*
* 
* 
* 
* 
* 
* 
�
p0
�1
�2
�3
�4
�5
�6
�7
�8
�9
�10
�11
�12
�13
�14
�15
�16
�17
�18
�19
�20
�21
�22
�23
�24
�25
�26
�27
�28
�29
�30
�31
�32*
SM
VARIABLE_VALUE	iteration0optimizer/_iterations/.ATTRIBUTES/VARIABLE_VALUE*
ZT
VARIABLE_VALUElearning_rate3optimizer/_learning_rate/.ATTRIBUTES/VARIABLE_VALUE*
* 
�
�0
�1
�2
�3
�4
�5
�6
�7
�8
�9
�10
�11
�12
�13
�14
�15*
�
�0
�1
�2
�3
�4
�5
�6
�7
�8
�9
�10
�11
�12
�13
�14
�15*
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
* 
<
�	variables
�	keras_api

�total

�count*
M
�	variables
�	keras_api

�total

�count
�
_fn_kwargs*
z
�	variables
�	keras_api
�true_positives
�true_negatives
�false_positives
�false_negatives*
z
�	variables
�	keras_api
�true_positives
�true_negatives
�false_positives
�false_negatives*
�~
VARIABLE_VALUE9Adam/m/dense_features/movieId_embedding/embedding_weights1optimizer/_variables/1/.ATTRIBUTES/VARIABLE_VALUE*
�~
VARIABLE_VALUE9Adam/v/dense_features/movieId_embedding/embedding_weights1optimizer/_variables/2/.ATTRIBUTES/VARIABLE_VALUE*
�
VARIABLE_VALUE:Adam/m/dense_features_1/userId_embedding/embedding_weights1optimizer/_variables/3/.ATTRIBUTES/VARIABLE_VALUE*
�
VARIABLE_VALUE:Adam/v/dense_features_1/userId_embedding/embedding_weights1optimizer/_variables/4/.ATTRIBUTES/VARIABLE_VALUE*
^X
VARIABLE_VALUEAdam/m/dense/kernel1optimizer/_variables/5/.ATTRIBUTES/VARIABLE_VALUE*
^X
VARIABLE_VALUEAdam/v/dense/kernel1optimizer/_variables/6/.ATTRIBUTES/VARIABLE_VALUE*
\V
VARIABLE_VALUEAdam/m/dense/bias1optimizer/_variables/7/.ATTRIBUTES/VARIABLE_VALUE*
\V
VARIABLE_VALUEAdam/v/dense/bias1optimizer/_variables/8/.ATTRIBUTES/VARIABLE_VALUE*
`Z
VARIABLE_VALUEAdam/m/dense_1/kernel1optimizer/_variables/9/.ATTRIBUTES/VARIABLE_VALUE*
a[
VARIABLE_VALUEAdam/v/dense_1/kernel2optimizer/_variables/10/.ATTRIBUTES/VARIABLE_VALUE*
_Y
VARIABLE_VALUEAdam/m/dense_1/bias2optimizer/_variables/11/.ATTRIBUTES/VARIABLE_VALUE*
_Y
VARIABLE_VALUEAdam/v/dense_1/bias2optimizer/_variables/12/.ATTRIBUTES/VARIABLE_VALUE*
a[
VARIABLE_VALUEAdam/m/dense_2/kernel2optimizer/_variables/13/.ATTRIBUTES/VARIABLE_VALUE*
a[
VARIABLE_VALUEAdam/v/dense_2/kernel2optimizer/_variables/14/.ATTRIBUTES/VARIABLE_VALUE*
_Y
VARIABLE_VALUEAdam/m/dense_2/bias2optimizer/_variables/15/.ATTRIBUTES/VARIABLE_VALUE*
_Y
VARIABLE_VALUEAdam/v/dense_2/bias2optimizer/_variables/16/.ATTRIBUTES/VARIABLE_VALUE*
a[
VARIABLE_VALUEAdam/m/dense_3/kernel2optimizer/_variables/17/.ATTRIBUTES/VARIABLE_VALUE*
a[
VARIABLE_VALUEAdam/v/dense_3/kernel2optimizer/_variables/18/.ATTRIBUTES/VARIABLE_VALUE*
_Y
VARIABLE_VALUEAdam/m/dense_3/bias2optimizer/_variables/19/.ATTRIBUTES/VARIABLE_VALUE*
_Y
VARIABLE_VALUEAdam/v/dense_3/bias2optimizer/_variables/20/.ATTRIBUTES/VARIABLE_VALUE*
a[
VARIABLE_VALUEAdam/m/dense_4/kernel2optimizer/_variables/21/.ATTRIBUTES/VARIABLE_VALUE*
a[
VARIABLE_VALUEAdam/v/dense_4/kernel2optimizer/_variables/22/.ATTRIBUTES/VARIABLE_VALUE*
_Y
VARIABLE_VALUEAdam/m/dense_4/bias2optimizer/_variables/23/.ATTRIBUTES/VARIABLE_VALUE*
_Y
VARIABLE_VALUEAdam/v/dense_4/bias2optimizer/_variables/24/.ATTRIBUTES/VARIABLE_VALUE*
a[
VARIABLE_VALUEAdam/m/dense_5/kernel2optimizer/_variables/25/.ATTRIBUTES/VARIABLE_VALUE*
a[
VARIABLE_VALUEAdam/v/dense_5/kernel2optimizer/_variables/26/.ATTRIBUTES/VARIABLE_VALUE*
_Y
VARIABLE_VALUEAdam/m/dense_5/bias2optimizer/_variables/27/.ATTRIBUTES/VARIABLE_VALUE*
_Y
VARIABLE_VALUEAdam/v/dense_5/bias2optimizer/_variables/28/.ATTRIBUTES/VARIABLE_VALUE*
a[
VARIABLE_VALUEAdam/m/dense_6/kernel2optimizer/_variables/29/.ATTRIBUTES/VARIABLE_VALUE*
a[
VARIABLE_VALUEAdam/v/dense_6/kernel2optimizer/_variables/30/.ATTRIBUTES/VARIABLE_VALUE*
_Y
VARIABLE_VALUEAdam/m/dense_6/bias2optimizer/_variables/31/.ATTRIBUTES/VARIABLE_VALUE*
_Y
VARIABLE_VALUEAdam/v/dense_6/bias2optimizer/_variables/32/.ATTRIBUTES/VARIABLE_VALUE*

�0
�1*

�	variables*
UO
VARIABLE_VALUEtotal_14keras_api/metrics/0/total/.ATTRIBUTES/VARIABLE_VALUE*
UO
VARIABLE_VALUEcount_14keras_api/metrics/0/count/.ATTRIBUTES/VARIABLE_VALUE*

�0
�1*

�	variables*
SM
VARIABLE_VALUEtotal4keras_api/metrics/1/total/.ATTRIBUTES/VARIABLE_VALUE*
SM
VARIABLE_VALUEcount4keras_api/metrics/1/count/.ATTRIBUTES/VARIABLE_VALUE*
* 
$
�0
�1
�2
�3*

�	variables*
ga
VARIABLE_VALUEtrue_positives_1=keras_api/metrics/2/true_positives/.ATTRIBUTES/VARIABLE_VALUE*
ga
VARIABLE_VALUEtrue_negatives_1=keras_api/metrics/2/true_negatives/.ATTRIBUTES/VARIABLE_VALUE*
ic
VARIABLE_VALUEfalse_positives_1>keras_api/metrics/2/false_positives/.ATTRIBUTES/VARIABLE_VALUE*
ic
VARIABLE_VALUEfalse_negatives_1>keras_api/metrics/2/false_negatives/.ATTRIBUTES/VARIABLE_VALUE*
$
�0
�1
�2
�3*

�	variables*
e_
VARIABLE_VALUEtrue_positives=keras_api/metrics/3/true_positives/.ATTRIBUTES/VARIABLE_VALUE*
e_
VARIABLE_VALUEtrue_negatives=keras_api/metrics/3/true_negatives/.ATTRIBUTES/VARIABLE_VALUE*
ga
VARIABLE_VALUEfalse_positives>keras_api/metrics/3/false_positives/.ATTRIBUTES/VARIABLE_VALUE*
ga
VARIABLE_VALUEfalse_negatives>keras_api/metrics/3/false_negatives/.ATTRIBUTES/VARIABLE_VALUE*
O
saver_filenamePlaceholder*
_output_shapes
: *
dtype0*
shape: 
�
StatefulPartitionedCall_1StatefulPartitionedCallsaver_filename2dense_features/movieId_embedding/embedding_weights3dense_features_1/userId_embedding/embedding_weightsdense/kernel
dense/biasdense_1/kerneldense_1/biasdense_2/kerneldense_2/biasdense_3/kerneldense_3/biasdense_4/kerneldense_4/biasdense_5/kerneldense_5/biasdense_6/kerneldense_6/bias	iterationlearning_rate9Adam/m/dense_features/movieId_embedding/embedding_weights9Adam/v/dense_features/movieId_embedding/embedding_weights:Adam/m/dense_features_1/userId_embedding/embedding_weights:Adam/v/dense_features_1/userId_embedding/embedding_weightsAdam/m/dense/kernelAdam/v/dense/kernelAdam/m/dense/biasAdam/v/dense/biasAdam/m/dense_1/kernelAdam/v/dense_1/kernelAdam/m/dense_1/biasAdam/v/dense_1/biasAdam/m/dense_2/kernelAdam/v/dense_2/kernelAdam/m/dense_2/biasAdam/v/dense_2/biasAdam/m/dense_3/kernelAdam/v/dense_3/kernelAdam/m/dense_3/biasAdam/v/dense_3/biasAdam/m/dense_4/kernelAdam/v/dense_4/kernelAdam/m/dense_4/biasAdam/v/dense_4/biasAdam/m/dense_5/kernelAdam/v/dense_5/kernelAdam/m/dense_5/biasAdam/v/dense_5/biasAdam/m/dense_6/kernelAdam/v/dense_6/kernelAdam/m/dense_6/biasAdam/v/dense_6/biastotal_1count_1totalcounttrue_positives_1true_negatives_1false_positives_1false_negatives_1true_positivestrue_negativesfalse_positivesfalse_negativesConst*K
TinD
B2@*
Tout
2*
_collective_manager_ids
 *
_output_shapes
: * 
_read_only_resource_inputs
 *-
config_proto

CPU

GPU 2J 8� *(
f#R!
__inference__traced_save_404194
�
StatefulPartitionedCall_2StatefulPartitionedCallsaver_filename2dense_features/movieId_embedding/embedding_weights3dense_features_1/userId_embedding/embedding_weightsdense/kernel
dense/biasdense_1/kerneldense_1/biasdense_2/kerneldense_2/biasdense_3/kerneldense_3/biasdense_4/kerneldense_4/biasdense_5/kerneldense_5/biasdense_6/kerneldense_6/bias	iterationlearning_rate9Adam/m/dense_features/movieId_embedding/embedding_weights9Adam/v/dense_features/movieId_embedding/embedding_weights:Adam/m/dense_features_1/userId_embedding/embedding_weights:Adam/v/dense_features_1/userId_embedding/embedding_weightsAdam/m/dense/kernelAdam/v/dense/kernelAdam/m/dense/biasAdam/v/dense/biasAdam/m/dense_1/kernelAdam/v/dense_1/kernelAdam/m/dense_1/biasAdam/v/dense_1/biasAdam/m/dense_2/kernelAdam/v/dense_2/kernelAdam/m/dense_2/biasAdam/v/dense_2/biasAdam/m/dense_3/kernelAdam/v/dense_3/kernelAdam/m/dense_3/biasAdam/v/dense_3/biasAdam/m/dense_4/kernelAdam/v/dense_4/kernelAdam/m/dense_4/biasAdam/v/dense_4/biasAdam/m/dense_5/kernelAdam/v/dense_5/kernelAdam/m/dense_5/biasAdam/v/dense_5/biasAdam/m/dense_6/kernelAdam/v/dense_6/kernelAdam/m/dense_6/biasAdam/v/dense_6/biastotal_1count_1totalcounttrue_positives_1true_negatives_1false_positives_1false_negatives_1true_positivestrue_negativesfalse_positivesfalse_negatives*J
TinC
A2?*
Tout
2*
_collective_manager_ids
 *
_output_shapes
: * 
_read_only_resource_inputs
 *-
config_proto

CPU

GPU 2J 8� *+
f&R$
"__inference__traced_restore_404389��
�

�
C__inference_dense_3_layer_call_and_return_conditional_losses_403721

inputs0
matmul_readvariableop_resource: @-
biasadd_readvariableop_resource:@
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpt
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes

: @*
dtype0i
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������@r
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes
:@*
dtype0v
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������@P
ReluReluBiasAdd:output:0*
T0*'
_output_shapes
:���������@a
IdentityIdentityRelu:activations:0^NoOp*
T0*'
_output_shapes
:���������@S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:��������� : : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:��������� 
 
_user_specified_nameinputs
�
�
$__inference_signature_wrapper_403273
movieid

userid
unknown:
��

	unknown_0:
��

	unknown_1:
 
	unknown_2: 
	unknown_3:
 
	unknown_4: 
	unknown_5: @
	unknown_6:@
	unknown_7: @
	unknown_8:@
	unknown_9:	@�

unknown_10:	�

unknown_11:	@�

unknown_12:	�

unknown_13:

unknown_14:
identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallmovieiduseridunknown	unknown_0	unknown_1	unknown_2	unknown_3	unknown_4	unknown_5	unknown_6	unknown_7	unknown_8	unknown_9
unknown_10
unknown_11
unknown_12
unknown_13
unknown_14*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������*2
_read_only_resource_inputs
	
*-
config_proto

CPU

GPU 2J 8� **
f%R#
!__inference__wrapped_model_402536o
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:���������<
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*Q
_input_shapes@
>:���������:���������: : : : : : : : : : : : : : : : 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403269:&"
 
_user_specified_name403267:&"
 
_user_specified_name403265:&"
 
_user_specified_name403263:&"
 
_user_specified_name403261:&"
 
_user_specified_name403259:&"
 
_user_specified_name403257:&
"
 
_user_specified_name403255:&	"
 
_user_specified_name403253:&"
 
_user_specified_name403251:&"
 
_user_specified_name403249:&"
 
_user_specified_name403247:&"
 
_user_specified_name403245:&"
 
_user_specified_name403243:&"
 
_user_specified_name403241:&"
 
_user_specified_name403239:KG
#
_output_shapes
:���������
 
_user_specified_nameuserId:L H
#
_output_shapes
:���������
!
_user_specified_name	movieId
�

�
C__inference_dense_5_layer_call_and_return_conditional_losses_402804

inputs1
matmul_readvariableop_resource:	@�.
biasadd_readvariableop_resource:	�
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpu
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes
:	@�*
dtype0j
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*(
_output_shapes
:����������s
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes	
:�*
dtype0w
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*(
_output_shapes
:����������Q
ReluReluBiasAdd:output:0*
T0*(
_output_shapes
:����������b
IdentityIdentityRelu:activations:0^NoOp*
T0*(
_output_shapes
:����������S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������@: : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:���������@
 
_user_specified_nameinputs
�|
�
J__inference_dense_features_layer_call_and_return_conditional_losses_403457
features_movieid
features_userido
[movieid_embedding_movieid_embedding_weights_embedding_lookup_sparse_embedding_lookup_403418:
��

identity��TmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupk
 movieId_embedding/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
����������
movieId_embedding/ExpandDims
ExpandDimsfeatures_movieid)movieId_embedding/ExpandDims/dim:output:0*
T0*'
_output_shapes
:���������{
0movieId_embedding/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB :
����������
*movieId_embedding/to_sparse_input/NotEqualNotEqual%movieId_embedding/ExpandDims:output:09movieId_embedding/to_sparse_input/ignore_value/x:output:0*
T0*'
_output_shapes
:����������
)movieId_embedding/to_sparse_input/indicesWhere.movieId_embedding/to_sparse_input/NotEqual:z:0*'
_output_shapes
:����������
(movieId_embedding/to_sparse_input/valuesGatherNd%movieId_embedding/ExpandDims:output:01movieId_embedding/to_sparse_input/indices:index:0*
Tindices0	*
Tparams0*#
_output_shapes
:����������
-movieId_embedding/to_sparse_input/dense_shapeShape%movieId_embedding/ExpandDims:output:0*
T0*
_output_shapes
:*
out_type0	:���
movieId_embedding/valuesCast1movieId_embedding/to_sparse_input/values:output:0*

DstT0	*

SrcT0*#
_output_shapes
:����������
7movieId_embedding/movieId_embedding_weights/Slice/beginConst*
_output_shapes
:*
dtype0*
valueB: �
6movieId_embedding/movieId_embedding_weights/Slice/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
1movieId_embedding/movieId_embedding_weights/SliceSlice6movieId_embedding/to_sparse_input/dense_shape:output:0@movieId_embedding/movieId_embedding_weights/Slice/begin:output:0?movieId_embedding/movieId_embedding_weights/Slice/size:output:0*
Index0*
T0	*
_output_shapes
:{
1movieId_embedding/movieId_embedding_weights/ConstConst*
_output_shapes
:*
dtype0*
valueB: �
0movieId_embedding/movieId_embedding_weights/ProdProd:movieId_embedding/movieId_embedding_weights/Slice:output:0:movieId_embedding/movieId_embedding_weights/Const:output:0*
T0	*
_output_shapes
: ~
<movieId_embedding/movieId_embedding_weights/GatherV2/indicesConst*
_output_shapes
: *
dtype0*
value	B :{
9movieId_embedding/movieId_embedding_weights/GatherV2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
4movieId_embedding/movieId_embedding_weights/GatherV2GatherV26movieId_embedding/to_sparse_input/dense_shape:output:0EmovieId_embedding/movieId_embedding_weights/GatherV2/indices:output:0BmovieId_embedding/movieId_embedding_weights/GatherV2/axis:output:0*
Taxis0*
Tindices0*
Tparams0	*
_output_shapes
: �
2movieId_embedding/movieId_embedding_weights/Cast/xPack9movieId_embedding/movieId_embedding_weights/Prod:output:0=movieId_embedding/movieId_embedding_weights/GatherV2:output:0*
N*
T0	*
_output_shapes
:�
9movieId_embedding/movieId_embedding_weights/SparseReshapeSparseReshape1movieId_embedding/to_sparse_input/indices:index:06movieId_embedding/to_sparse_input/dense_shape:output:0;movieId_embedding/movieId_embedding_weights/Cast/x:output:0*-
_output_shapes
:���������:�
BmovieId_embedding/movieId_embedding_weights/SparseReshape/IdentityIdentitymovieId_embedding/values:y:0*
T0	*#
_output_shapes
:���������|
:movieId_embedding/movieId_embedding_weights/GreaterEqual/yConst*
_output_shapes
: *
dtype0	*
value	B	 R �
8movieId_embedding/movieId_embedding_weights/GreaterEqualGreaterEqualKmovieId_embedding/movieId_embedding_weights/SparseReshape/Identity:output:0CmovieId_embedding/movieId_embedding_weights/GreaterEqual/y:output:0*
T0	*#
_output_shapes
:����������
1movieId_embedding/movieId_embedding_weights/WhereWhere<movieId_embedding/movieId_embedding_weights/GreaterEqual:z:0*'
_output_shapes
:����������
9movieId_embedding/movieId_embedding_weights/Reshape/shapeConst*
_output_shapes
:*
dtype0*
valueB:
����������
3movieId_embedding/movieId_embedding_weights/ReshapeReshape9movieId_embedding/movieId_embedding_weights/Where:index:0BmovieId_embedding/movieId_embedding_weights/Reshape/shape:output:0*
T0	*#
_output_shapes
:���������}
;movieId_embedding/movieId_embedding_weights/GatherV2_1/axisConst*
_output_shapes
: *
dtype0*
value	B : �
6movieId_embedding/movieId_embedding_weights/GatherV2_1GatherV2JmovieId_embedding/movieId_embedding_weights/SparseReshape:output_indices:0<movieId_embedding/movieId_embedding_weights/Reshape:output:0DmovieId_embedding/movieId_embedding_weights/GatherV2_1/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*'
_output_shapes
:���������}
;movieId_embedding/movieId_embedding_weights/GatherV2_2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
6movieId_embedding/movieId_embedding_weights/GatherV2_2GatherV2KmovieId_embedding/movieId_embedding_weights/SparseReshape/Identity:output:0<movieId_embedding/movieId_embedding_weights/Reshape:output:0DmovieId_embedding/movieId_embedding_weights/GatherV2_2/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*#
_output_shapes
:����������
4movieId_embedding/movieId_embedding_weights/IdentityIdentityHmovieId_embedding/movieId_embedding_weights/SparseReshape:output_shape:0*
T0	*
_output_shapes
:�
EmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/ConstConst*
_output_shapes
: *
dtype0	*
value	B	 R �
SmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRowsSparseFillEmptyRows?movieId_embedding/movieId_embedding_weights/GatherV2_1:output:0?movieId_embedding/movieId_embedding_weights/GatherV2_2:output:0=movieId_embedding/movieId_embedding_weights/Identity:output:0NmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/Const:output:0*
T0	*T
_output_shapesB
@:���������:���������:���������:����������
WmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB"        �
YmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB"       �
YmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB"      �
QmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_sliceStridedSlicedmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_indices:0`movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack:output:0bmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1:output:0bmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2:output:0*
Index0*
T0	*#
_output_shapes
:���������*

begin_mask*
end_mask*
shrink_axis_mask�
JmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/UniqueUniquecmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_values:0*
T0	*2
_output_shapes 
:���������:����������
TmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupResourceGather[movieid_embedding_movieid_embedding_weights_embedding_lookup_sparse_embedding_lookup_403418NmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/Unique:y:0*
Tindices0	*n
_classd
b`loc:@movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/403418*'
_output_shapes
:���������
*
dtype0�
]movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/IdentityIdentity]movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup:output:0*
T0*'
_output_shapes
:���������
�
CmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparseSparseSegmentMeanfmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/Identity:output:0PmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/Unique:idx:0ZmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice:output:0*
Tsegmentids0	*
T0*'
_output_shapes
:���������
�
;movieId_embedding/movieId_embedding_weights/Reshape_1/shapeConst*
_output_shapes
:*
dtype0*
valueB"����   �
5movieId_embedding/movieId_embedding_weights/Reshape_1ReshapeimovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:empty_row_indicator:0DmovieId_embedding/movieId_embedding_weights/Reshape_1/shape:output:0*
T0
*'
_output_shapes
:����������
1movieId_embedding/movieId_embedding_weights/ShapeShapeLmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*
_output_shapes
::���
?movieId_embedding/movieId_embedding_weights/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB:�
AmovieId_embedding/movieId_embedding_weights/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:�
AmovieId_embedding/movieId_embedding_weights/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
9movieId_embedding/movieId_embedding_weights/strided_sliceStridedSlice:movieId_embedding/movieId_embedding_weights/Shape:output:0HmovieId_embedding/movieId_embedding_weights/strided_slice/stack:output:0JmovieId_embedding/movieId_embedding_weights/strided_slice/stack_1:output:0JmovieId_embedding/movieId_embedding_weights/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_masku
3movieId_embedding/movieId_embedding_weights/stack/0Const*
_output_shapes
: *
dtype0*
value	B :�
1movieId_embedding/movieId_embedding_weights/stackPack<movieId_embedding/movieId_embedding_weights/stack/0:output:0BmovieId_embedding/movieId_embedding_weights/strided_slice:output:0*
N*
T0*
_output_shapes
:�
0movieId_embedding/movieId_embedding_weights/TileTile>movieId_embedding/movieId_embedding_weights/Reshape_1:output:0:movieId_embedding/movieId_embedding_weights/stack:output:0*
T0
*'
_output_shapes
:���������
�
6movieId_embedding/movieId_embedding_weights/zeros_like	ZerosLikeLmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
+movieId_embedding/movieId_embedding_weightsSelect9movieId_embedding/movieId_embedding_weights/Tile:output:0:movieId_embedding/movieId_embedding_weights/zeros_like:y:0LmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
2movieId_embedding/movieId_embedding_weights/Cast_1Cast6movieId_embedding/to_sparse_input/dense_shape:output:0*

DstT0*

SrcT0	*
_output_shapes
:�
9movieId_embedding/movieId_embedding_weights/Slice_1/beginConst*
_output_shapes
:*
dtype0*
valueB: �
8movieId_embedding/movieId_embedding_weights/Slice_1/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
3movieId_embedding/movieId_embedding_weights/Slice_1Slice6movieId_embedding/movieId_embedding_weights/Cast_1:y:0BmovieId_embedding/movieId_embedding_weights/Slice_1/begin:output:0AmovieId_embedding/movieId_embedding_weights/Slice_1/size:output:0*
Index0*
T0*
_output_shapes
:�
3movieId_embedding/movieId_embedding_weights/Shape_1Shape4movieId_embedding/movieId_embedding_weights:output:0*
T0*
_output_shapes
::���
9movieId_embedding/movieId_embedding_weights/Slice_2/beginConst*
_output_shapes
:*
dtype0*
valueB:�
8movieId_embedding/movieId_embedding_weights/Slice_2/sizeConst*
_output_shapes
:*
dtype0*
valueB:
����������
3movieId_embedding/movieId_embedding_weights/Slice_2Slice<movieId_embedding/movieId_embedding_weights/Shape_1:output:0BmovieId_embedding/movieId_embedding_weights/Slice_2/begin:output:0AmovieId_embedding/movieId_embedding_weights/Slice_2/size:output:0*
Index0*
T0*
_output_shapes
:y
7movieId_embedding/movieId_embedding_weights/concat/axisConst*
_output_shapes
: *
dtype0*
value	B : �
2movieId_embedding/movieId_embedding_weights/concatConcatV2<movieId_embedding/movieId_embedding_weights/Slice_1:output:0<movieId_embedding/movieId_embedding_weights/Slice_2:output:0@movieId_embedding/movieId_embedding_weights/concat/axis:output:0*
N*
T0*
_output_shapes
:�
5movieId_embedding/movieId_embedding_weights/Reshape_2Reshape4movieId_embedding/movieId_embedding_weights:output:0;movieId_embedding/movieId_embedding_weights/concat:output:0*
T0*'
_output_shapes
:���������
�
movieId_embedding/ShapeShape>movieId_embedding/movieId_embedding_weights/Reshape_2:output:0*
T0*
_output_shapes
::��o
%movieId_embedding/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: q
'movieId_embedding/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:q
'movieId_embedding/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
movieId_embedding/strided_sliceStridedSlice movieId_embedding/Shape:output:0.movieId_embedding/strided_slice/stack:output:00movieId_embedding/strided_slice/stack_1:output:00movieId_embedding/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_maskc
!movieId_embedding/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
�
movieId_embedding/Reshape/shapePack(movieId_embedding/strided_slice:output:0*movieId_embedding/Reshape/shape/1:output:0*
N*
T0*
_output_shapes
:�
movieId_embedding/ReshapeReshape>movieId_embedding/movieId_embedding_weights/Reshape_2:output:0(movieId_embedding/Reshape/shape:output:0*
T0*'
_output_shapes
:���������
\
concat/concat_dimConst*
_output_shapes
: *
dtype0*
valueB :
���������o
concat/concatIdentity"movieId_embedding/Reshape:output:0*
T0*'
_output_shapes
:���������
e
IdentityIdentityconcat/concat:output:0^NoOp*
T0*'
_output_shapes
:���������
y
NoOpNoOpU^movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*3
_input_shapes"
 :���������:���������: 2�
TmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupTmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup:&"
 
_user_specified_name403418:TP
#
_output_shapes
:���������
)
_user_specified_namefeatures_userid:U Q
#
_output_shapes
:���������
*
_user_specified_namefeatures_movieid
�

�
C__inference_dense_2_layer_call_and_return_conditional_losses_402772

inputs0
matmul_readvariableop_resource: @-
biasadd_readvariableop_resource:@
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpt
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes

: @*
dtype0i
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������@r
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes
:@*
dtype0v
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������@P
ReluReluBiasAdd:output:0*
T0*'
_output_shapes
:���������@a
IdentityIdentityRelu:activations:0^NoOp*
T0*'
_output_shapes
:���������@S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:��������� : : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:��������� 
 
_user_specified_nameinputs
�6
�
A__inference_model_layer_call_and_return_conditional_losses_402840
movieid

userid+
dense_features_1_402624:
��
)
dense_features_402711:
��
 
dense_1_402725:
 
dense_1_402727: 
dense_402741:
 
dense_402743:  
dense_3_402757: @
dense_3_402759:@ 
dense_2_402773: @
dense_2_402775:@!
dense_4_402789:	@�
dense_4_402791:	�!
dense_5_402805:	@�
dense_5_402807:	� 
dense_6_402834:
dense_6_402836:
identity��dense/StatefulPartitionedCall�dense_1/StatefulPartitionedCall�dense_2/StatefulPartitionedCall�dense_3/StatefulPartitionedCall�dense_4/StatefulPartitionedCall�dense_5/StatefulPartitionedCall�dense_6/StatefulPartitionedCall�&dense_features/StatefulPartitionedCall�(dense_features_1/StatefulPartitionedCall�
(dense_features_1/StatefulPartitionedCallStatefulPartitionedCallmovieiduseriddense_features_1_402624*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������
*#
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *U
fPRN
L__inference_dense_features_1_layer_call_and_return_conditional_losses_402623�
&dense_features/StatefulPartitionedCallStatefulPartitionedCallmovieiduseriddense_features_402711*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������
*#
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *S
fNRL
J__inference_dense_features_layer_call_and_return_conditional_losses_402710�
dense_1/StatefulPartitionedCallStatefulPartitionedCall1dense_features_1/StatefulPartitionedCall:output:0dense_1_402725dense_1_402727*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:��������� *$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_1_layer_call_and_return_conditional_losses_402724�
dense/StatefulPartitionedCallStatefulPartitionedCall/dense_features/StatefulPartitionedCall:output:0dense_402741dense_402743*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:��������� *$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *J
fERC
A__inference_dense_layer_call_and_return_conditional_losses_402740�
dense_3/StatefulPartitionedCallStatefulPartitionedCall(dense_1/StatefulPartitionedCall:output:0dense_3_402757dense_3_402759*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������@*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_3_layer_call_and_return_conditional_losses_402756�
dense_2/StatefulPartitionedCallStatefulPartitionedCall&dense/StatefulPartitionedCall:output:0dense_2_402773dense_2_402775*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������@*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_2_layer_call_and_return_conditional_losses_402772�
dense_4/StatefulPartitionedCallStatefulPartitionedCall(dense_2/StatefulPartitionedCall:output:0dense_4_402789dense_4_402791*
Tin
2*
Tout
2*
_collective_manager_ids
 *(
_output_shapes
:����������*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_4_layer_call_and_return_conditional_losses_402788�
dense_5/StatefulPartitionedCallStatefulPartitionedCall(dense_3/StatefulPartitionedCall:output:0dense_5_402805dense_5_402807*
Tin
2*
Tout
2*
_collective_manager_ids
 *(
_output_shapes
:����������*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_5_layer_call_and_return_conditional_losses_402804�
dot/PartitionedCallPartitionedCall(dense_4/StatefulPartitionedCall:output:0(dense_5/StatefulPartitionedCall:output:0*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������* 
_read_only_resource_inputs
 *-
config_proto

CPU

GPU 2J 8� *H
fCRA
?__inference_dot_layer_call_and_return_conditional_losses_402821�
dense_6/StatefulPartitionedCallStatefulPartitionedCalldot/PartitionedCall:output:0dense_6_402834dense_6_402836*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_6_layer_call_and_return_conditional_losses_402833w
IdentityIdentity(dense_6/StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:����������
NoOpNoOp^dense/StatefulPartitionedCall ^dense_1/StatefulPartitionedCall ^dense_2/StatefulPartitionedCall ^dense_3/StatefulPartitionedCall ^dense_4/StatefulPartitionedCall ^dense_5/StatefulPartitionedCall ^dense_6/StatefulPartitionedCall'^dense_features/StatefulPartitionedCall)^dense_features_1/StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*Q
_input_shapes@
>:���������:���������: : : : : : : : : : : : : : : : 2>
dense/StatefulPartitionedCalldense/StatefulPartitionedCall2B
dense_1/StatefulPartitionedCalldense_1/StatefulPartitionedCall2B
dense_2/StatefulPartitionedCalldense_2/StatefulPartitionedCall2B
dense_3/StatefulPartitionedCalldense_3/StatefulPartitionedCall2B
dense_4/StatefulPartitionedCalldense_4/StatefulPartitionedCall2B
dense_5/StatefulPartitionedCalldense_5/StatefulPartitionedCall2B
dense_6/StatefulPartitionedCalldense_6/StatefulPartitionedCall2P
&dense_features/StatefulPartitionedCall&dense_features/StatefulPartitionedCall2T
(dense_features_1/StatefulPartitionedCall(dense_features_1/StatefulPartitionedCall:&"
 
_user_specified_name402836:&"
 
_user_specified_name402834:&"
 
_user_specified_name402807:&"
 
_user_specified_name402805:&"
 
_user_specified_name402791:&"
 
_user_specified_name402789:&"
 
_user_specified_name402775:&
"
 
_user_specified_name402773:&	"
 
_user_specified_name402759:&"
 
_user_specified_name402757:&"
 
_user_specified_name402743:&"
 
_user_specified_name402741:&"
 
_user_specified_name402727:&"
 
_user_specified_name402725:&"
 
_user_specified_name402711:&"
 
_user_specified_name402624:KG
#
_output_shapes
:���������
 
_user_specified_nameuserId:L H
#
_output_shapes
:���������
!
_user_specified_name	movieId
�
�
(__inference_dense_3_layer_call_fn_403710

inputs
unknown: @
	unknown_0:@
identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallinputsunknown	unknown_0*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������@*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_3_layer_call_and_return_conditional_losses_402756o
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:���������@<
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:��������� : : 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403706:&"
 
_user_specified_name403704:O K
'
_output_shapes
:��������� 
 
_user_specified_nameinputs
�
�
&__inference_model_layer_call_fn_403131
movieid

userid
unknown:
��

	unknown_0:
��

	unknown_1:
 
	unknown_2: 
	unknown_3:
 
	unknown_4: 
	unknown_5: @
	unknown_6:@
	unknown_7: @
	unknown_8:@
	unknown_9:	@�

unknown_10:	�

unknown_11:	@�

unknown_12:	�

unknown_13:

unknown_14:
identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallmovieiduseridunknown	unknown_0	unknown_1	unknown_2	unknown_3	unknown_4	unknown_5	unknown_6	unknown_7	unknown_8	unknown_9
unknown_10
unknown_11
unknown_12
unknown_13
unknown_14*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������*2
_read_only_resource_inputs
	
*-
config_proto

CPU

GPU 2J 8� *J
fERC
A__inference_model_layer_call_and_return_conditional_losses_403055o
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:���������<
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*Q
_input_shapes@
>:���������:���������: : : : : : : : : : : : : : : : 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403127:&"
 
_user_specified_name403125:&"
 
_user_specified_name403123:&"
 
_user_specified_name403121:&"
 
_user_specified_name403119:&"
 
_user_specified_name403117:&"
 
_user_specified_name403115:&
"
 
_user_specified_name403113:&	"
 
_user_specified_name403111:&"
 
_user_specified_name403109:&"
 
_user_specified_name403107:&"
 
_user_specified_name403105:&"
 
_user_specified_name403103:&"
 
_user_specified_name403101:&"
 
_user_specified_name403099:&"
 
_user_specified_name403097:KG
#
_output_shapes
:���������
 
_user_specified_nameuserId:L H
#
_output_shapes
:���������
!
_user_specified_name	movieId
�
�
&__inference_dense_layer_call_fn_403650

inputs
unknown:
 
	unknown_0: 
identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallinputsunknown	unknown_0*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:��������� *$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *J
fERC
A__inference_dense_layer_call_and_return_conditional_losses_402740o
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:��������� <
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������
: : 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403646:&"
 
_user_specified_name403644:O K
'
_output_shapes
:���������

 
_user_specified_nameinputs
��
�'
"__inference__traced_restore_404389
file_prefixW
Cassignvariableop_dense_features_movieid_embedding_embedding_weights:
��
Z
Fassignvariableop_1_dense_features_1_userid_embedding_embedding_weights:
��
1
assignvariableop_2_dense_kernel:
 +
assignvariableop_3_dense_bias: 3
!assignvariableop_4_dense_1_kernel:
 -
assignvariableop_5_dense_1_bias: 3
!assignvariableop_6_dense_2_kernel: @-
assignvariableop_7_dense_2_bias:@3
!assignvariableop_8_dense_3_kernel: @-
assignvariableop_9_dense_3_bias:@5
"assignvariableop_10_dense_4_kernel:	@�/
 assignvariableop_11_dense_4_bias:	�5
"assignvariableop_12_dense_5_kernel:	@�/
 assignvariableop_13_dense_5_bias:	�4
"assignvariableop_14_dense_6_kernel:.
 assignvariableop_15_dense_6_bias:'
assignvariableop_16_iteration:	 +
!assignvariableop_17_learning_rate: a
Massignvariableop_18_adam_m_dense_features_movieid_embedding_embedding_weights:
��
a
Massignvariableop_19_adam_v_dense_features_movieid_embedding_embedding_weights:
��
b
Nassignvariableop_20_adam_m_dense_features_1_userid_embedding_embedding_weights:
��
b
Nassignvariableop_21_adam_v_dense_features_1_userid_embedding_embedding_weights:
��
9
'assignvariableop_22_adam_m_dense_kernel:
 9
'assignvariableop_23_adam_v_dense_kernel:
 3
%assignvariableop_24_adam_m_dense_bias: 3
%assignvariableop_25_adam_v_dense_bias: ;
)assignvariableop_26_adam_m_dense_1_kernel:
 ;
)assignvariableop_27_adam_v_dense_1_kernel:
 5
'assignvariableop_28_adam_m_dense_1_bias: 5
'assignvariableop_29_adam_v_dense_1_bias: ;
)assignvariableop_30_adam_m_dense_2_kernel: @;
)assignvariableop_31_adam_v_dense_2_kernel: @5
'assignvariableop_32_adam_m_dense_2_bias:@5
'assignvariableop_33_adam_v_dense_2_bias:@;
)assignvariableop_34_adam_m_dense_3_kernel: @;
)assignvariableop_35_adam_v_dense_3_kernel: @5
'assignvariableop_36_adam_m_dense_3_bias:@5
'assignvariableop_37_adam_v_dense_3_bias:@<
)assignvariableop_38_adam_m_dense_4_kernel:	@�<
)assignvariableop_39_adam_v_dense_4_kernel:	@�6
'assignvariableop_40_adam_m_dense_4_bias:	�6
'assignvariableop_41_adam_v_dense_4_bias:	�<
)assignvariableop_42_adam_m_dense_5_kernel:	@�<
)assignvariableop_43_adam_v_dense_5_kernel:	@�6
'assignvariableop_44_adam_m_dense_5_bias:	�6
'assignvariableop_45_adam_v_dense_5_bias:	�;
)assignvariableop_46_adam_m_dense_6_kernel:;
)assignvariableop_47_adam_v_dense_6_kernel:5
'assignvariableop_48_adam_m_dense_6_bias:5
'assignvariableop_49_adam_v_dense_6_bias:%
assignvariableop_50_total_1: %
assignvariableop_51_count_1: #
assignvariableop_52_total: #
assignvariableop_53_count: 3
$assignvariableop_54_true_positives_1:	�3
$assignvariableop_55_true_negatives_1:	�4
%assignvariableop_56_false_positives_1:	�4
%assignvariableop_57_false_negatives_1:	�1
"assignvariableop_58_true_positives:	�1
"assignvariableop_59_true_negatives:	�2
#assignvariableop_60_false_positives:	�2
#assignvariableop_61_false_negatives:	�
identity_63��AssignVariableOp�AssignVariableOp_1�AssignVariableOp_10�AssignVariableOp_11�AssignVariableOp_12�AssignVariableOp_13�AssignVariableOp_14�AssignVariableOp_15�AssignVariableOp_16�AssignVariableOp_17�AssignVariableOp_18�AssignVariableOp_19�AssignVariableOp_2�AssignVariableOp_20�AssignVariableOp_21�AssignVariableOp_22�AssignVariableOp_23�AssignVariableOp_24�AssignVariableOp_25�AssignVariableOp_26�AssignVariableOp_27�AssignVariableOp_28�AssignVariableOp_29�AssignVariableOp_3�AssignVariableOp_30�AssignVariableOp_31�AssignVariableOp_32�AssignVariableOp_33�AssignVariableOp_34�AssignVariableOp_35�AssignVariableOp_36�AssignVariableOp_37�AssignVariableOp_38�AssignVariableOp_39�AssignVariableOp_4�AssignVariableOp_40�AssignVariableOp_41�AssignVariableOp_42�AssignVariableOp_43�AssignVariableOp_44�AssignVariableOp_45�AssignVariableOp_46�AssignVariableOp_47�AssignVariableOp_48�AssignVariableOp_49�AssignVariableOp_5�AssignVariableOp_50�AssignVariableOp_51�AssignVariableOp_52�AssignVariableOp_53�AssignVariableOp_54�AssignVariableOp_55�AssignVariableOp_56�AssignVariableOp_57�AssignVariableOp_58�AssignVariableOp_59�AssignVariableOp_6�AssignVariableOp_60�AssignVariableOp_61�AssignVariableOp_7�AssignVariableOp_8�AssignVariableOp_9�
RestoreV2/tensor_namesConst"/device:CPU:0*
_output_shapes
:?*
dtype0*�
value�B�?BTlayer_with_weights-0/movieId_embedding.Sembedding_weights/.ATTRIBUTES/VARIABLE_VALUEBSlayer_with_weights-1/userId_embedding.Sembedding_weights/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-2/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-2/bias/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-3/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-3/bias/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-4/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-4/bias/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-5/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-5/bias/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-6/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-6/bias/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-7/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-7/bias/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-8/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-8/bias/.ATTRIBUTES/VARIABLE_VALUEB0optimizer/_iterations/.ATTRIBUTES/VARIABLE_VALUEB3optimizer/_learning_rate/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/1/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/2/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/3/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/4/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/5/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/6/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/7/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/8/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/9/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/10/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/11/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/12/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/13/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/14/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/15/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/16/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/17/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/18/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/19/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/20/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/21/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/22/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/23/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/24/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/25/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/26/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/27/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/28/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/29/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/30/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/31/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/32/.ATTRIBUTES/VARIABLE_VALUEB4keras_api/metrics/0/total/.ATTRIBUTES/VARIABLE_VALUEB4keras_api/metrics/0/count/.ATTRIBUTES/VARIABLE_VALUEB4keras_api/metrics/1/total/.ATTRIBUTES/VARIABLE_VALUEB4keras_api/metrics/1/count/.ATTRIBUTES/VARIABLE_VALUEB=keras_api/metrics/2/true_positives/.ATTRIBUTES/VARIABLE_VALUEB=keras_api/metrics/2/true_negatives/.ATTRIBUTES/VARIABLE_VALUEB>keras_api/metrics/2/false_positives/.ATTRIBUTES/VARIABLE_VALUEB>keras_api/metrics/2/false_negatives/.ATTRIBUTES/VARIABLE_VALUEB=keras_api/metrics/3/true_positives/.ATTRIBUTES/VARIABLE_VALUEB=keras_api/metrics/3/true_negatives/.ATTRIBUTES/VARIABLE_VALUEB>keras_api/metrics/3/false_positives/.ATTRIBUTES/VARIABLE_VALUEB>keras_api/metrics/3/false_negatives/.ATTRIBUTES/VARIABLE_VALUEB_CHECKPOINTABLE_OBJECT_GRAPH�
RestoreV2/shape_and_slicesConst"/device:CPU:0*
_output_shapes
:?*
dtype0*�
value�B�?B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B �
	RestoreV2	RestoreV2file_prefixRestoreV2/tensor_names:output:0#RestoreV2/shape_and_slices:output:0"/device:CPU:0*�
_output_shapes�
�:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*M
dtypesC
A2?	[
IdentityIdentityRestoreV2:tensors:0"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOpAssignVariableOpCassignvariableop_dense_features_movieid_embedding_embedding_weightsIdentity:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0]

Identity_1IdentityRestoreV2:tensors:1"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_1AssignVariableOpFassignvariableop_1_dense_features_1_userid_embedding_embedding_weightsIdentity_1:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0]

Identity_2IdentityRestoreV2:tensors:2"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_2AssignVariableOpassignvariableop_2_dense_kernelIdentity_2:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0]

Identity_3IdentityRestoreV2:tensors:3"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_3AssignVariableOpassignvariableop_3_dense_biasIdentity_3:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0]

Identity_4IdentityRestoreV2:tensors:4"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_4AssignVariableOp!assignvariableop_4_dense_1_kernelIdentity_4:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0]

Identity_5IdentityRestoreV2:tensors:5"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_5AssignVariableOpassignvariableop_5_dense_1_biasIdentity_5:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0]

Identity_6IdentityRestoreV2:tensors:6"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_6AssignVariableOp!assignvariableop_6_dense_2_kernelIdentity_6:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0]

Identity_7IdentityRestoreV2:tensors:7"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_7AssignVariableOpassignvariableop_7_dense_2_biasIdentity_7:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0]

Identity_8IdentityRestoreV2:tensors:8"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_8AssignVariableOp!assignvariableop_8_dense_3_kernelIdentity_8:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0]

Identity_9IdentityRestoreV2:tensors:9"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_9AssignVariableOpassignvariableop_9_dense_3_biasIdentity_9:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_10IdentityRestoreV2:tensors:10"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_10AssignVariableOp"assignvariableop_10_dense_4_kernelIdentity_10:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_11IdentityRestoreV2:tensors:11"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_11AssignVariableOp assignvariableop_11_dense_4_biasIdentity_11:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_12IdentityRestoreV2:tensors:12"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_12AssignVariableOp"assignvariableop_12_dense_5_kernelIdentity_12:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_13IdentityRestoreV2:tensors:13"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_13AssignVariableOp assignvariableop_13_dense_5_biasIdentity_13:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_14IdentityRestoreV2:tensors:14"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_14AssignVariableOp"assignvariableop_14_dense_6_kernelIdentity_14:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_15IdentityRestoreV2:tensors:15"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_15AssignVariableOp assignvariableop_15_dense_6_biasIdentity_15:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_16IdentityRestoreV2:tensors:16"/device:CPU:0*
T0	*
_output_shapes
:�
AssignVariableOp_16AssignVariableOpassignvariableop_16_iterationIdentity_16:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0	_
Identity_17IdentityRestoreV2:tensors:17"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_17AssignVariableOp!assignvariableop_17_learning_rateIdentity_17:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_18IdentityRestoreV2:tensors:18"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_18AssignVariableOpMassignvariableop_18_adam_m_dense_features_movieid_embedding_embedding_weightsIdentity_18:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_19IdentityRestoreV2:tensors:19"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_19AssignVariableOpMassignvariableop_19_adam_v_dense_features_movieid_embedding_embedding_weightsIdentity_19:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_20IdentityRestoreV2:tensors:20"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_20AssignVariableOpNassignvariableop_20_adam_m_dense_features_1_userid_embedding_embedding_weightsIdentity_20:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_21IdentityRestoreV2:tensors:21"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_21AssignVariableOpNassignvariableop_21_adam_v_dense_features_1_userid_embedding_embedding_weightsIdentity_21:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_22IdentityRestoreV2:tensors:22"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_22AssignVariableOp'assignvariableop_22_adam_m_dense_kernelIdentity_22:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_23IdentityRestoreV2:tensors:23"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_23AssignVariableOp'assignvariableop_23_adam_v_dense_kernelIdentity_23:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_24IdentityRestoreV2:tensors:24"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_24AssignVariableOp%assignvariableop_24_adam_m_dense_biasIdentity_24:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_25IdentityRestoreV2:tensors:25"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_25AssignVariableOp%assignvariableop_25_adam_v_dense_biasIdentity_25:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_26IdentityRestoreV2:tensors:26"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_26AssignVariableOp)assignvariableop_26_adam_m_dense_1_kernelIdentity_26:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_27IdentityRestoreV2:tensors:27"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_27AssignVariableOp)assignvariableop_27_adam_v_dense_1_kernelIdentity_27:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_28IdentityRestoreV2:tensors:28"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_28AssignVariableOp'assignvariableop_28_adam_m_dense_1_biasIdentity_28:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_29IdentityRestoreV2:tensors:29"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_29AssignVariableOp'assignvariableop_29_adam_v_dense_1_biasIdentity_29:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_30IdentityRestoreV2:tensors:30"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_30AssignVariableOp)assignvariableop_30_adam_m_dense_2_kernelIdentity_30:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_31IdentityRestoreV2:tensors:31"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_31AssignVariableOp)assignvariableop_31_adam_v_dense_2_kernelIdentity_31:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_32IdentityRestoreV2:tensors:32"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_32AssignVariableOp'assignvariableop_32_adam_m_dense_2_biasIdentity_32:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_33IdentityRestoreV2:tensors:33"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_33AssignVariableOp'assignvariableop_33_adam_v_dense_2_biasIdentity_33:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_34IdentityRestoreV2:tensors:34"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_34AssignVariableOp)assignvariableop_34_adam_m_dense_3_kernelIdentity_34:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_35IdentityRestoreV2:tensors:35"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_35AssignVariableOp)assignvariableop_35_adam_v_dense_3_kernelIdentity_35:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_36IdentityRestoreV2:tensors:36"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_36AssignVariableOp'assignvariableop_36_adam_m_dense_3_biasIdentity_36:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_37IdentityRestoreV2:tensors:37"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_37AssignVariableOp'assignvariableop_37_adam_v_dense_3_biasIdentity_37:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_38IdentityRestoreV2:tensors:38"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_38AssignVariableOp)assignvariableop_38_adam_m_dense_4_kernelIdentity_38:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_39IdentityRestoreV2:tensors:39"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_39AssignVariableOp)assignvariableop_39_adam_v_dense_4_kernelIdentity_39:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_40IdentityRestoreV2:tensors:40"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_40AssignVariableOp'assignvariableop_40_adam_m_dense_4_biasIdentity_40:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_41IdentityRestoreV2:tensors:41"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_41AssignVariableOp'assignvariableop_41_adam_v_dense_4_biasIdentity_41:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_42IdentityRestoreV2:tensors:42"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_42AssignVariableOp)assignvariableop_42_adam_m_dense_5_kernelIdentity_42:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_43IdentityRestoreV2:tensors:43"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_43AssignVariableOp)assignvariableop_43_adam_v_dense_5_kernelIdentity_43:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_44IdentityRestoreV2:tensors:44"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_44AssignVariableOp'assignvariableop_44_adam_m_dense_5_biasIdentity_44:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_45IdentityRestoreV2:tensors:45"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_45AssignVariableOp'assignvariableop_45_adam_v_dense_5_biasIdentity_45:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_46IdentityRestoreV2:tensors:46"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_46AssignVariableOp)assignvariableop_46_adam_m_dense_6_kernelIdentity_46:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_47IdentityRestoreV2:tensors:47"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_47AssignVariableOp)assignvariableop_47_adam_v_dense_6_kernelIdentity_47:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_48IdentityRestoreV2:tensors:48"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_48AssignVariableOp'assignvariableop_48_adam_m_dense_6_biasIdentity_48:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_49IdentityRestoreV2:tensors:49"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_49AssignVariableOp'assignvariableop_49_adam_v_dense_6_biasIdentity_49:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_50IdentityRestoreV2:tensors:50"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_50AssignVariableOpassignvariableop_50_total_1Identity_50:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_51IdentityRestoreV2:tensors:51"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_51AssignVariableOpassignvariableop_51_count_1Identity_51:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_52IdentityRestoreV2:tensors:52"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_52AssignVariableOpassignvariableop_52_totalIdentity_52:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_53IdentityRestoreV2:tensors:53"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_53AssignVariableOpassignvariableop_53_countIdentity_53:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_54IdentityRestoreV2:tensors:54"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_54AssignVariableOp$assignvariableop_54_true_positives_1Identity_54:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_55IdentityRestoreV2:tensors:55"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_55AssignVariableOp$assignvariableop_55_true_negatives_1Identity_55:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_56IdentityRestoreV2:tensors:56"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_56AssignVariableOp%assignvariableop_56_false_positives_1Identity_56:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_57IdentityRestoreV2:tensors:57"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_57AssignVariableOp%assignvariableop_57_false_negatives_1Identity_57:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_58IdentityRestoreV2:tensors:58"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_58AssignVariableOp"assignvariableop_58_true_positivesIdentity_58:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_59IdentityRestoreV2:tensors:59"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_59AssignVariableOp"assignvariableop_59_true_negativesIdentity_59:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_60IdentityRestoreV2:tensors:60"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_60AssignVariableOp#assignvariableop_60_false_positivesIdentity_60:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0_
Identity_61IdentityRestoreV2:tensors:61"/device:CPU:0*
T0*
_output_shapes
:�
AssignVariableOp_61AssignVariableOp#assignvariableop_61_false_negativesIdentity_61:output:0"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *
dtype0Y
NoOpNoOp"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 �
Identity_62Identityfile_prefix^AssignVariableOp^AssignVariableOp_1^AssignVariableOp_10^AssignVariableOp_11^AssignVariableOp_12^AssignVariableOp_13^AssignVariableOp_14^AssignVariableOp_15^AssignVariableOp_16^AssignVariableOp_17^AssignVariableOp_18^AssignVariableOp_19^AssignVariableOp_2^AssignVariableOp_20^AssignVariableOp_21^AssignVariableOp_22^AssignVariableOp_23^AssignVariableOp_24^AssignVariableOp_25^AssignVariableOp_26^AssignVariableOp_27^AssignVariableOp_28^AssignVariableOp_29^AssignVariableOp_3^AssignVariableOp_30^AssignVariableOp_31^AssignVariableOp_32^AssignVariableOp_33^AssignVariableOp_34^AssignVariableOp_35^AssignVariableOp_36^AssignVariableOp_37^AssignVariableOp_38^AssignVariableOp_39^AssignVariableOp_4^AssignVariableOp_40^AssignVariableOp_41^AssignVariableOp_42^AssignVariableOp_43^AssignVariableOp_44^AssignVariableOp_45^AssignVariableOp_46^AssignVariableOp_47^AssignVariableOp_48^AssignVariableOp_49^AssignVariableOp_5^AssignVariableOp_50^AssignVariableOp_51^AssignVariableOp_52^AssignVariableOp_53^AssignVariableOp_54^AssignVariableOp_55^AssignVariableOp_56^AssignVariableOp_57^AssignVariableOp_58^AssignVariableOp_59^AssignVariableOp_6^AssignVariableOp_60^AssignVariableOp_61^AssignVariableOp_7^AssignVariableOp_8^AssignVariableOp_9^NoOp"/device:CPU:0*
T0*
_output_shapes
: W
Identity_63IdentityIdentity_62:output:0^NoOp_1*
T0*
_output_shapes
: �

NoOp_1NoOp^AssignVariableOp^AssignVariableOp_1^AssignVariableOp_10^AssignVariableOp_11^AssignVariableOp_12^AssignVariableOp_13^AssignVariableOp_14^AssignVariableOp_15^AssignVariableOp_16^AssignVariableOp_17^AssignVariableOp_18^AssignVariableOp_19^AssignVariableOp_2^AssignVariableOp_20^AssignVariableOp_21^AssignVariableOp_22^AssignVariableOp_23^AssignVariableOp_24^AssignVariableOp_25^AssignVariableOp_26^AssignVariableOp_27^AssignVariableOp_28^AssignVariableOp_29^AssignVariableOp_3^AssignVariableOp_30^AssignVariableOp_31^AssignVariableOp_32^AssignVariableOp_33^AssignVariableOp_34^AssignVariableOp_35^AssignVariableOp_36^AssignVariableOp_37^AssignVariableOp_38^AssignVariableOp_39^AssignVariableOp_4^AssignVariableOp_40^AssignVariableOp_41^AssignVariableOp_42^AssignVariableOp_43^AssignVariableOp_44^AssignVariableOp_45^AssignVariableOp_46^AssignVariableOp_47^AssignVariableOp_48^AssignVariableOp_49^AssignVariableOp_5^AssignVariableOp_50^AssignVariableOp_51^AssignVariableOp_52^AssignVariableOp_53^AssignVariableOp_54^AssignVariableOp_55^AssignVariableOp_56^AssignVariableOp_57^AssignVariableOp_58^AssignVariableOp_59^AssignVariableOp_6^AssignVariableOp_60^AssignVariableOp_61^AssignVariableOp_7^AssignVariableOp_8^AssignVariableOp_9*
_output_shapes
 "#
identity_63Identity_63:output:0*(
_construction_contextkEagerRuntime*�
_input_shapes�
~: : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : 2*
AssignVariableOp_10AssignVariableOp_102*
AssignVariableOp_11AssignVariableOp_112*
AssignVariableOp_12AssignVariableOp_122*
AssignVariableOp_13AssignVariableOp_132*
AssignVariableOp_14AssignVariableOp_142*
AssignVariableOp_15AssignVariableOp_152*
AssignVariableOp_16AssignVariableOp_162*
AssignVariableOp_17AssignVariableOp_172*
AssignVariableOp_18AssignVariableOp_182*
AssignVariableOp_19AssignVariableOp_192(
AssignVariableOp_1AssignVariableOp_12*
AssignVariableOp_20AssignVariableOp_202*
AssignVariableOp_21AssignVariableOp_212*
AssignVariableOp_22AssignVariableOp_222*
AssignVariableOp_23AssignVariableOp_232*
AssignVariableOp_24AssignVariableOp_242*
AssignVariableOp_25AssignVariableOp_252*
AssignVariableOp_26AssignVariableOp_262*
AssignVariableOp_27AssignVariableOp_272*
AssignVariableOp_28AssignVariableOp_282*
AssignVariableOp_29AssignVariableOp_292(
AssignVariableOp_2AssignVariableOp_22*
AssignVariableOp_30AssignVariableOp_302*
AssignVariableOp_31AssignVariableOp_312*
AssignVariableOp_32AssignVariableOp_322*
AssignVariableOp_33AssignVariableOp_332*
AssignVariableOp_34AssignVariableOp_342*
AssignVariableOp_35AssignVariableOp_352*
AssignVariableOp_36AssignVariableOp_362*
AssignVariableOp_37AssignVariableOp_372*
AssignVariableOp_38AssignVariableOp_382*
AssignVariableOp_39AssignVariableOp_392(
AssignVariableOp_3AssignVariableOp_32*
AssignVariableOp_40AssignVariableOp_402*
AssignVariableOp_41AssignVariableOp_412*
AssignVariableOp_42AssignVariableOp_422*
AssignVariableOp_43AssignVariableOp_432*
AssignVariableOp_44AssignVariableOp_442*
AssignVariableOp_45AssignVariableOp_452*
AssignVariableOp_46AssignVariableOp_462*
AssignVariableOp_47AssignVariableOp_472*
AssignVariableOp_48AssignVariableOp_482*
AssignVariableOp_49AssignVariableOp_492(
AssignVariableOp_4AssignVariableOp_42*
AssignVariableOp_50AssignVariableOp_502*
AssignVariableOp_51AssignVariableOp_512*
AssignVariableOp_52AssignVariableOp_522*
AssignVariableOp_53AssignVariableOp_532*
AssignVariableOp_54AssignVariableOp_542*
AssignVariableOp_55AssignVariableOp_552*
AssignVariableOp_56AssignVariableOp_562*
AssignVariableOp_57AssignVariableOp_572*
AssignVariableOp_58AssignVariableOp_582*
AssignVariableOp_59AssignVariableOp_592(
AssignVariableOp_5AssignVariableOp_52*
AssignVariableOp_60AssignVariableOp_602*
AssignVariableOp_61AssignVariableOp_612(
AssignVariableOp_6AssignVariableOp_62(
AssignVariableOp_7AssignVariableOp_72(
AssignVariableOp_8AssignVariableOp_82(
AssignVariableOp_9AssignVariableOp_92$
AssignVariableOpAssignVariableOp:/>+
)
_user_specified_namefalse_negatives:/=+
)
_user_specified_namefalse_positives:.<*
(
_user_specified_nametrue_negatives:.;*
(
_user_specified_nametrue_positives:1:-
+
_user_specified_namefalse_negatives_1:19-
+
_user_specified_namefalse_positives_1:08,
*
_user_specified_nametrue_negatives_1:07,
*
_user_specified_nametrue_positives_1:%6!

_user_specified_namecount:%5!

_user_specified_nametotal:'4#
!
_user_specified_name	count_1:'3#
!
_user_specified_name	total_1:32/
-
_user_specified_nameAdam/v/dense_6/bias:31/
-
_user_specified_nameAdam/m/dense_6/bias:501
/
_user_specified_nameAdam/v/dense_6/kernel:5/1
/
_user_specified_nameAdam/m/dense_6/kernel:3./
-
_user_specified_nameAdam/v/dense_5/bias:3-/
-
_user_specified_nameAdam/m/dense_5/bias:5,1
/
_user_specified_nameAdam/v/dense_5/kernel:5+1
/
_user_specified_nameAdam/m/dense_5/kernel:3*/
-
_user_specified_nameAdam/v/dense_4/bias:3)/
-
_user_specified_nameAdam/m/dense_4/bias:5(1
/
_user_specified_nameAdam/v/dense_4/kernel:5'1
/
_user_specified_nameAdam/m/dense_4/kernel:3&/
-
_user_specified_nameAdam/v/dense_3/bias:3%/
-
_user_specified_nameAdam/m/dense_3/bias:5$1
/
_user_specified_nameAdam/v/dense_3/kernel:5#1
/
_user_specified_nameAdam/m/dense_3/kernel:3"/
-
_user_specified_nameAdam/v/dense_2/bias:3!/
-
_user_specified_nameAdam/m/dense_2/bias:5 1
/
_user_specified_nameAdam/v/dense_2/kernel:51
/
_user_specified_nameAdam/m/dense_2/kernel:3/
-
_user_specified_nameAdam/v/dense_1/bias:3/
-
_user_specified_nameAdam/m/dense_1/bias:51
/
_user_specified_nameAdam/v/dense_1/kernel:51
/
_user_specified_nameAdam/m/dense_1/kernel:1-
+
_user_specified_nameAdam/v/dense/bias:1-
+
_user_specified_nameAdam/m/dense/bias:3/
-
_user_specified_nameAdam/v/dense/kernel:3/
-
_user_specified_nameAdam/m/dense/kernel:ZV
T
_user_specified_name<:Adam/v/dense_features_1/userId_embedding/embedding_weights:ZV
T
_user_specified_name<:Adam/m/dense_features_1/userId_embedding/embedding_weights:YU
S
_user_specified_name;9Adam/v/dense_features/movieId_embedding/embedding_weights:YU
S
_user_specified_name;9Adam/m/dense_features/movieId_embedding/embedding_weights:-)
'
_user_specified_namelearning_rate:)%
#
_user_specified_name	iteration:,(
&
_user_specified_namedense_6/bias:.*
(
_user_specified_namedense_6/kernel:,(
&
_user_specified_namedense_5/bias:.*
(
_user_specified_namedense_5/kernel:,(
&
_user_specified_namedense_4/bias:.*
(
_user_specified_namedense_4/kernel:,
(
&
_user_specified_namedense_3/bias:.	*
(
_user_specified_namedense_3/kernel:,(
&
_user_specified_namedense_2/bias:.*
(
_user_specified_namedense_2/kernel:,(
&
_user_specified_namedense_1/bias:.*
(
_user_specified_namedense_1/kernel:*&
$
_user_specified_name
dense/bias:,(
&
_user_specified_namedense/kernel:SO
M
_user_specified_name53dense_features_1/userId_embedding/embedding_weights:RN
L
_user_specified_name42dense_features/movieId_embedding/embedding_weights:C ?

_output_shapes
: 
%
_user_specified_namefile_prefix
�

�
C__inference_dense_3_layer_call_and_return_conditional_losses_402756

inputs0
matmul_readvariableop_resource: @-
biasadd_readvariableop_resource:@
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpt
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes

: @*
dtype0i
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������@r
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes
:@*
dtype0v
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������@P
ReluReluBiasAdd:output:0*
T0*'
_output_shapes
:���������@a
IdentityIdentityRelu:activations:0^NoOp*
T0*'
_output_shapes
:���������@S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:��������� : : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:��������� 
 
_user_specified_nameinputs
�

�
C__inference_dense_6_layer_call_and_return_conditional_losses_403799

inputs0
matmul_readvariableop_resource:-
biasadd_readvariableop_resource:
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpt
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes

:*
dtype0i
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������r
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes
:*
dtype0v
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������V
SigmoidSigmoidBiasAdd:output:0*
T0*'
_output_shapes
:���������Z
IdentityIdentitySigmoid:y:0^NoOp*
T0*'
_output_shapes
:���������S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������: : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:���������
 
_user_specified_nameinputs
�

�
C__inference_dense_4_layer_call_and_return_conditional_losses_402788

inputs1
matmul_readvariableop_resource:	@�.
biasadd_readvariableop_resource:	�
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpu
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes
:	@�*
dtype0j
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*(
_output_shapes
:����������s
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes	
:�*
dtype0w
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*(
_output_shapes
:����������Q
ReluReluBiasAdd:output:0*
T0*(
_output_shapes
:����������b
IdentityIdentityRelu:activations:0^NoOp*
T0*(
_output_shapes
:����������S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������@: : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:���������@
 
_user_specified_nameinputs
�
�
(__inference_dense_6_layer_call_fn_403788

inputs
unknown:
	unknown_0:
identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallinputsunknown	unknown_0*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_6_layer_call_and_return_conditional_losses_402833o
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:���������<
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������: : 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403784:&"
 
_user_specified_name403782:O K
'
_output_shapes
:���������
 
_user_specified_nameinputs
�

�
C__inference_dense_2_layer_call_and_return_conditional_losses_403701

inputs0
matmul_readvariableop_resource: @-
biasadd_readvariableop_resource:@
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpt
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes

: @*
dtype0i
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������@r
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes
:@*
dtype0v
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������@P
ReluReluBiasAdd:output:0*
T0*'
_output_shapes
:���������@a
IdentityIdentityRelu:activations:0^NoOp*
T0*'
_output_shapes
:���������@S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:��������� : : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:��������� 
 
_user_specified_nameinputs
�

�
A__inference_dense_layer_call_and_return_conditional_losses_403661

inputs0
matmul_readvariableop_resource:
 -
biasadd_readvariableop_resource: 
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpt
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes

:
 *
dtype0i
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:��������� r
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes
: *
dtype0v
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:��������� P
ReluReluBiasAdd:output:0*
T0*'
_output_shapes
:��������� a
IdentityIdentityRelu:activations:0^NoOp*
T0*'
_output_shapes
:��������� S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������
: : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:���������

 
_user_specified_nameinputs
�|
�
J__inference_dense_features_layer_call_and_return_conditional_losses_403014
features

features_1o
[movieid_embedding_movieid_embedding_weights_embedding_lookup_sparse_embedding_lookup_402975:
��

identity��TmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupk
 movieId_embedding/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
����������
movieId_embedding/ExpandDims
ExpandDimsfeatures)movieId_embedding/ExpandDims/dim:output:0*
T0*'
_output_shapes
:���������{
0movieId_embedding/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB :
����������
*movieId_embedding/to_sparse_input/NotEqualNotEqual%movieId_embedding/ExpandDims:output:09movieId_embedding/to_sparse_input/ignore_value/x:output:0*
T0*'
_output_shapes
:����������
)movieId_embedding/to_sparse_input/indicesWhere.movieId_embedding/to_sparse_input/NotEqual:z:0*'
_output_shapes
:����������
(movieId_embedding/to_sparse_input/valuesGatherNd%movieId_embedding/ExpandDims:output:01movieId_embedding/to_sparse_input/indices:index:0*
Tindices0	*
Tparams0*#
_output_shapes
:����������
-movieId_embedding/to_sparse_input/dense_shapeShape%movieId_embedding/ExpandDims:output:0*
T0*
_output_shapes
:*
out_type0	:���
movieId_embedding/valuesCast1movieId_embedding/to_sparse_input/values:output:0*

DstT0	*

SrcT0*#
_output_shapes
:����������
7movieId_embedding/movieId_embedding_weights/Slice/beginConst*
_output_shapes
:*
dtype0*
valueB: �
6movieId_embedding/movieId_embedding_weights/Slice/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
1movieId_embedding/movieId_embedding_weights/SliceSlice6movieId_embedding/to_sparse_input/dense_shape:output:0@movieId_embedding/movieId_embedding_weights/Slice/begin:output:0?movieId_embedding/movieId_embedding_weights/Slice/size:output:0*
Index0*
T0	*
_output_shapes
:{
1movieId_embedding/movieId_embedding_weights/ConstConst*
_output_shapes
:*
dtype0*
valueB: �
0movieId_embedding/movieId_embedding_weights/ProdProd:movieId_embedding/movieId_embedding_weights/Slice:output:0:movieId_embedding/movieId_embedding_weights/Const:output:0*
T0	*
_output_shapes
: ~
<movieId_embedding/movieId_embedding_weights/GatherV2/indicesConst*
_output_shapes
: *
dtype0*
value	B :{
9movieId_embedding/movieId_embedding_weights/GatherV2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
4movieId_embedding/movieId_embedding_weights/GatherV2GatherV26movieId_embedding/to_sparse_input/dense_shape:output:0EmovieId_embedding/movieId_embedding_weights/GatherV2/indices:output:0BmovieId_embedding/movieId_embedding_weights/GatherV2/axis:output:0*
Taxis0*
Tindices0*
Tparams0	*
_output_shapes
: �
2movieId_embedding/movieId_embedding_weights/Cast/xPack9movieId_embedding/movieId_embedding_weights/Prod:output:0=movieId_embedding/movieId_embedding_weights/GatherV2:output:0*
N*
T0	*
_output_shapes
:�
9movieId_embedding/movieId_embedding_weights/SparseReshapeSparseReshape1movieId_embedding/to_sparse_input/indices:index:06movieId_embedding/to_sparse_input/dense_shape:output:0;movieId_embedding/movieId_embedding_weights/Cast/x:output:0*-
_output_shapes
:���������:�
BmovieId_embedding/movieId_embedding_weights/SparseReshape/IdentityIdentitymovieId_embedding/values:y:0*
T0	*#
_output_shapes
:���������|
:movieId_embedding/movieId_embedding_weights/GreaterEqual/yConst*
_output_shapes
: *
dtype0	*
value	B	 R �
8movieId_embedding/movieId_embedding_weights/GreaterEqualGreaterEqualKmovieId_embedding/movieId_embedding_weights/SparseReshape/Identity:output:0CmovieId_embedding/movieId_embedding_weights/GreaterEqual/y:output:0*
T0	*#
_output_shapes
:����������
1movieId_embedding/movieId_embedding_weights/WhereWhere<movieId_embedding/movieId_embedding_weights/GreaterEqual:z:0*'
_output_shapes
:����������
9movieId_embedding/movieId_embedding_weights/Reshape/shapeConst*
_output_shapes
:*
dtype0*
valueB:
����������
3movieId_embedding/movieId_embedding_weights/ReshapeReshape9movieId_embedding/movieId_embedding_weights/Where:index:0BmovieId_embedding/movieId_embedding_weights/Reshape/shape:output:0*
T0	*#
_output_shapes
:���������}
;movieId_embedding/movieId_embedding_weights/GatherV2_1/axisConst*
_output_shapes
: *
dtype0*
value	B : �
6movieId_embedding/movieId_embedding_weights/GatherV2_1GatherV2JmovieId_embedding/movieId_embedding_weights/SparseReshape:output_indices:0<movieId_embedding/movieId_embedding_weights/Reshape:output:0DmovieId_embedding/movieId_embedding_weights/GatherV2_1/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*'
_output_shapes
:���������}
;movieId_embedding/movieId_embedding_weights/GatherV2_2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
6movieId_embedding/movieId_embedding_weights/GatherV2_2GatherV2KmovieId_embedding/movieId_embedding_weights/SparseReshape/Identity:output:0<movieId_embedding/movieId_embedding_weights/Reshape:output:0DmovieId_embedding/movieId_embedding_weights/GatherV2_2/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*#
_output_shapes
:����������
4movieId_embedding/movieId_embedding_weights/IdentityIdentityHmovieId_embedding/movieId_embedding_weights/SparseReshape:output_shape:0*
T0	*
_output_shapes
:�
EmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/ConstConst*
_output_shapes
: *
dtype0	*
value	B	 R �
SmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRowsSparseFillEmptyRows?movieId_embedding/movieId_embedding_weights/GatherV2_1:output:0?movieId_embedding/movieId_embedding_weights/GatherV2_2:output:0=movieId_embedding/movieId_embedding_weights/Identity:output:0NmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/Const:output:0*
T0	*T
_output_shapesB
@:���������:���������:���������:����������
WmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB"        �
YmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB"       �
YmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB"      �
QmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_sliceStridedSlicedmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_indices:0`movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack:output:0bmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1:output:0bmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2:output:0*
Index0*
T0	*#
_output_shapes
:���������*

begin_mask*
end_mask*
shrink_axis_mask�
JmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/UniqueUniquecmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_values:0*
T0	*2
_output_shapes 
:���������:����������
TmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupResourceGather[movieid_embedding_movieid_embedding_weights_embedding_lookup_sparse_embedding_lookup_402975NmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/Unique:y:0*
Tindices0	*n
_classd
b`loc:@movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/402975*'
_output_shapes
:���������
*
dtype0�
]movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/IdentityIdentity]movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup:output:0*
T0*'
_output_shapes
:���������
�
CmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparseSparseSegmentMeanfmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/Identity:output:0PmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/Unique:idx:0ZmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice:output:0*
Tsegmentids0	*
T0*'
_output_shapes
:���������
�
;movieId_embedding/movieId_embedding_weights/Reshape_1/shapeConst*
_output_shapes
:*
dtype0*
valueB"����   �
5movieId_embedding/movieId_embedding_weights/Reshape_1ReshapeimovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:empty_row_indicator:0DmovieId_embedding/movieId_embedding_weights/Reshape_1/shape:output:0*
T0
*'
_output_shapes
:����������
1movieId_embedding/movieId_embedding_weights/ShapeShapeLmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*
_output_shapes
::���
?movieId_embedding/movieId_embedding_weights/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB:�
AmovieId_embedding/movieId_embedding_weights/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:�
AmovieId_embedding/movieId_embedding_weights/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
9movieId_embedding/movieId_embedding_weights/strided_sliceStridedSlice:movieId_embedding/movieId_embedding_weights/Shape:output:0HmovieId_embedding/movieId_embedding_weights/strided_slice/stack:output:0JmovieId_embedding/movieId_embedding_weights/strided_slice/stack_1:output:0JmovieId_embedding/movieId_embedding_weights/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_masku
3movieId_embedding/movieId_embedding_weights/stack/0Const*
_output_shapes
: *
dtype0*
value	B :�
1movieId_embedding/movieId_embedding_weights/stackPack<movieId_embedding/movieId_embedding_weights/stack/0:output:0BmovieId_embedding/movieId_embedding_weights/strided_slice:output:0*
N*
T0*
_output_shapes
:�
0movieId_embedding/movieId_embedding_weights/TileTile>movieId_embedding/movieId_embedding_weights/Reshape_1:output:0:movieId_embedding/movieId_embedding_weights/stack:output:0*
T0
*'
_output_shapes
:���������
�
6movieId_embedding/movieId_embedding_weights/zeros_like	ZerosLikeLmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
+movieId_embedding/movieId_embedding_weightsSelect9movieId_embedding/movieId_embedding_weights/Tile:output:0:movieId_embedding/movieId_embedding_weights/zeros_like:y:0LmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
2movieId_embedding/movieId_embedding_weights/Cast_1Cast6movieId_embedding/to_sparse_input/dense_shape:output:0*

DstT0*

SrcT0	*
_output_shapes
:�
9movieId_embedding/movieId_embedding_weights/Slice_1/beginConst*
_output_shapes
:*
dtype0*
valueB: �
8movieId_embedding/movieId_embedding_weights/Slice_1/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
3movieId_embedding/movieId_embedding_weights/Slice_1Slice6movieId_embedding/movieId_embedding_weights/Cast_1:y:0BmovieId_embedding/movieId_embedding_weights/Slice_1/begin:output:0AmovieId_embedding/movieId_embedding_weights/Slice_1/size:output:0*
Index0*
T0*
_output_shapes
:�
3movieId_embedding/movieId_embedding_weights/Shape_1Shape4movieId_embedding/movieId_embedding_weights:output:0*
T0*
_output_shapes
::���
9movieId_embedding/movieId_embedding_weights/Slice_2/beginConst*
_output_shapes
:*
dtype0*
valueB:�
8movieId_embedding/movieId_embedding_weights/Slice_2/sizeConst*
_output_shapes
:*
dtype0*
valueB:
����������
3movieId_embedding/movieId_embedding_weights/Slice_2Slice<movieId_embedding/movieId_embedding_weights/Shape_1:output:0BmovieId_embedding/movieId_embedding_weights/Slice_2/begin:output:0AmovieId_embedding/movieId_embedding_weights/Slice_2/size:output:0*
Index0*
T0*
_output_shapes
:y
7movieId_embedding/movieId_embedding_weights/concat/axisConst*
_output_shapes
: *
dtype0*
value	B : �
2movieId_embedding/movieId_embedding_weights/concatConcatV2<movieId_embedding/movieId_embedding_weights/Slice_1:output:0<movieId_embedding/movieId_embedding_weights/Slice_2:output:0@movieId_embedding/movieId_embedding_weights/concat/axis:output:0*
N*
T0*
_output_shapes
:�
5movieId_embedding/movieId_embedding_weights/Reshape_2Reshape4movieId_embedding/movieId_embedding_weights:output:0;movieId_embedding/movieId_embedding_weights/concat:output:0*
T0*'
_output_shapes
:���������
�
movieId_embedding/ShapeShape>movieId_embedding/movieId_embedding_weights/Reshape_2:output:0*
T0*
_output_shapes
::��o
%movieId_embedding/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: q
'movieId_embedding/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:q
'movieId_embedding/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
movieId_embedding/strided_sliceStridedSlice movieId_embedding/Shape:output:0.movieId_embedding/strided_slice/stack:output:00movieId_embedding/strided_slice/stack_1:output:00movieId_embedding/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_maskc
!movieId_embedding/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
�
movieId_embedding/Reshape/shapePack(movieId_embedding/strided_slice:output:0*movieId_embedding/Reshape/shape/1:output:0*
N*
T0*
_output_shapes
:�
movieId_embedding/ReshapeReshape>movieId_embedding/movieId_embedding_weights/Reshape_2:output:0(movieId_embedding/Reshape/shape:output:0*
T0*'
_output_shapes
:���������
\
concat/concat_dimConst*
_output_shapes
: *
dtype0*
valueB :
���������o
concat/concatIdentity"movieId_embedding/Reshape:output:0*
T0*'
_output_shapes
:���������
e
IdentityIdentityconcat/concat:output:0^NoOp*
T0*'
_output_shapes
:���������
y
NoOpNoOpU^movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*3
_input_shapes"
 :���������:���������: 2�
TmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupTmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup:&"
 
_user_specified_name402975:MI
#
_output_shapes
:���������
"
_user_specified_name
features:M I
#
_output_shapes
:���������
"
_user_specified_name
features
�

�
C__inference_dense_6_layer_call_and_return_conditional_losses_402833

inputs0
matmul_readvariableop_resource:-
biasadd_readvariableop_resource:
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpt
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes

:*
dtype0i
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������r
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes
:*
dtype0v
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������V
SigmoidSigmoidBiasAdd:output:0*
T0*'
_output_shapes
:���������Z
IdentityIdentitySigmoid:y:0^NoOp*
T0*'
_output_shapes
:���������S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������: : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:���������
 
_user_specified_nameinputs
�

�
C__inference_dense_5_layer_call_and_return_conditional_losses_403761

inputs1
matmul_readvariableop_resource:	@�.
biasadd_readvariableop_resource:	�
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpu
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes
:	@�*
dtype0j
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*(
_output_shapes
:����������s
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes	
:�*
dtype0w
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*(
_output_shapes
:����������Q
ReluReluBiasAdd:output:0*
T0*(
_output_shapes
:����������b
IdentityIdentityRelu:activations:0^NoOp*
T0*(
_output_shapes
:����������S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������@: : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:���������@
 
_user_specified_nameinputs
�

�
C__inference_dense_4_layer_call_and_return_conditional_losses_403741

inputs1
matmul_readvariableop_resource:	@�.
biasadd_readvariableop_resource:	�
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpu
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes
:	@�*
dtype0j
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*(
_output_shapes
:����������s
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes	
:�*
dtype0w
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*(
_output_shapes
:����������Q
ReluReluBiasAdd:output:0*
T0*(
_output_shapes
:����������b
IdentityIdentityRelu:activations:0^NoOp*
T0*(
_output_shapes
:����������S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������@: : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:���������@
 
_user_specified_nameinputs
�
�
(__inference_dense_1_layer_call_fn_403670

inputs
unknown:
 
	unknown_0: 
identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallinputsunknown	unknown_0*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:��������� *$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_1_layer_call_and_return_conditional_losses_402724o
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:��������� <
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������
: : 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403666:&"
 
_user_specified_name403664:O K
'
_output_shapes
:���������

 
_user_specified_nameinputs
�|
�
J__inference_dense_features_layer_call_and_return_conditional_losses_403373
features_movieid
features_userido
[movieid_embedding_movieid_embedding_weights_embedding_lookup_sparse_embedding_lookup_403334:
��

identity��TmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupk
 movieId_embedding/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
����������
movieId_embedding/ExpandDims
ExpandDimsfeatures_movieid)movieId_embedding/ExpandDims/dim:output:0*
T0*'
_output_shapes
:���������{
0movieId_embedding/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB :
����������
*movieId_embedding/to_sparse_input/NotEqualNotEqual%movieId_embedding/ExpandDims:output:09movieId_embedding/to_sparse_input/ignore_value/x:output:0*
T0*'
_output_shapes
:����������
)movieId_embedding/to_sparse_input/indicesWhere.movieId_embedding/to_sparse_input/NotEqual:z:0*'
_output_shapes
:����������
(movieId_embedding/to_sparse_input/valuesGatherNd%movieId_embedding/ExpandDims:output:01movieId_embedding/to_sparse_input/indices:index:0*
Tindices0	*
Tparams0*#
_output_shapes
:����������
-movieId_embedding/to_sparse_input/dense_shapeShape%movieId_embedding/ExpandDims:output:0*
T0*
_output_shapes
:*
out_type0	:���
movieId_embedding/valuesCast1movieId_embedding/to_sparse_input/values:output:0*

DstT0	*

SrcT0*#
_output_shapes
:����������
7movieId_embedding/movieId_embedding_weights/Slice/beginConst*
_output_shapes
:*
dtype0*
valueB: �
6movieId_embedding/movieId_embedding_weights/Slice/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
1movieId_embedding/movieId_embedding_weights/SliceSlice6movieId_embedding/to_sparse_input/dense_shape:output:0@movieId_embedding/movieId_embedding_weights/Slice/begin:output:0?movieId_embedding/movieId_embedding_weights/Slice/size:output:0*
Index0*
T0	*
_output_shapes
:{
1movieId_embedding/movieId_embedding_weights/ConstConst*
_output_shapes
:*
dtype0*
valueB: �
0movieId_embedding/movieId_embedding_weights/ProdProd:movieId_embedding/movieId_embedding_weights/Slice:output:0:movieId_embedding/movieId_embedding_weights/Const:output:0*
T0	*
_output_shapes
: ~
<movieId_embedding/movieId_embedding_weights/GatherV2/indicesConst*
_output_shapes
: *
dtype0*
value	B :{
9movieId_embedding/movieId_embedding_weights/GatherV2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
4movieId_embedding/movieId_embedding_weights/GatherV2GatherV26movieId_embedding/to_sparse_input/dense_shape:output:0EmovieId_embedding/movieId_embedding_weights/GatherV2/indices:output:0BmovieId_embedding/movieId_embedding_weights/GatherV2/axis:output:0*
Taxis0*
Tindices0*
Tparams0	*
_output_shapes
: �
2movieId_embedding/movieId_embedding_weights/Cast/xPack9movieId_embedding/movieId_embedding_weights/Prod:output:0=movieId_embedding/movieId_embedding_weights/GatherV2:output:0*
N*
T0	*
_output_shapes
:�
9movieId_embedding/movieId_embedding_weights/SparseReshapeSparseReshape1movieId_embedding/to_sparse_input/indices:index:06movieId_embedding/to_sparse_input/dense_shape:output:0;movieId_embedding/movieId_embedding_weights/Cast/x:output:0*-
_output_shapes
:���������:�
BmovieId_embedding/movieId_embedding_weights/SparseReshape/IdentityIdentitymovieId_embedding/values:y:0*
T0	*#
_output_shapes
:���������|
:movieId_embedding/movieId_embedding_weights/GreaterEqual/yConst*
_output_shapes
: *
dtype0	*
value	B	 R �
8movieId_embedding/movieId_embedding_weights/GreaterEqualGreaterEqualKmovieId_embedding/movieId_embedding_weights/SparseReshape/Identity:output:0CmovieId_embedding/movieId_embedding_weights/GreaterEqual/y:output:0*
T0	*#
_output_shapes
:����������
1movieId_embedding/movieId_embedding_weights/WhereWhere<movieId_embedding/movieId_embedding_weights/GreaterEqual:z:0*'
_output_shapes
:����������
9movieId_embedding/movieId_embedding_weights/Reshape/shapeConst*
_output_shapes
:*
dtype0*
valueB:
����������
3movieId_embedding/movieId_embedding_weights/ReshapeReshape9movieId_embedding/movieId_embedding_weights/Where:index:0BmovieId_embedding/movieId_embedding_weights/Reshape/shape:output:0*
T0	*#
_output_shapes
:���������}
;movieId_embedding/movieId_embedding_weights/GatherV2_1/axisConst*
_output_shapes
: *
dtype0*
value	B : �
6movieId_embedding/movieId_embedding_weights/GatherV2_1GatherV2JmovieId_embedding/movieId_embedding_weights/SparseReshape:output_indices:0<movieId_embedding/movieId_embedding_weights/Reshape:output:0DmovieId_embedding/movieId_embedding_weights/GatherV2_1/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*'
_output_shapes
:���������}
;movieId_embedding/movieId_embedding_weights/GatherV2_2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
6movieId_embedding/movieId_embedding_weights/GatherV2_2GatherV2KmovieId_embedding/movieId_embedding_weights/SparseReshape/Identity:output:0<movieId_embedding/movieId_embedding_weights/Reshape:output:0DmovieId_embedding/movieId_embedding_weights/GatherV2_2/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*#
_output_shapes
:����������
4movieId_embedding/movieId_embedding_weights/IdentityIdentityHmovieId_embedding/movieId_embedding_weights/SparseReshape:output_shape:0*
T0	*
_output_shapes
:�
EmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/ConstConst*
_output_shapes
: *
dtype0	*
value	B	 R �
SmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRowsSparseFillEmptyRows?movieId_embedding/movieId_embedding_weights/GatherV2_1:output:0?movieId_embedding/movieId_embedding_weights/GatherV2_2:output:0=movieId_embedding/movieId_embedding_weights/Identity:output:0NmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/Const:output:0*
T0	*T
_output_shapesB
@:���������:���������:���������:����������
WmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB"        �
YmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB"       �
YmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB"      �
QmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_sliceStridedSlicedmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_indices:0`movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack:output:0bmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1:output:0bmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2:output:0*
Index0*
T0	*#
_output_shapes
:���������*

begin_mask*
end_mask*
shrink_axis_mask�
JmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/UniqueUniquecmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_values:0*
T0	*2
_output_shapes 
:���������:����������
TmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupResourceGather[movieid_embedding_movieid_embedding_weights_embedding_lookup_sparse_embedding_lookup_403334NmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/Unique:y:0*
Tindices0	*n
_classd
b`loc:@movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/403334*'
_output_shapes
:���������
*
dtype0�
]movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/IdentityIdentity]movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup:output:0*
T0*'
_output_shapes
:���������
�
CmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparseSparseSegmentMeanfmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/Identity:output:0PmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/Unique:idx:0ZmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice:output:0*
Tsegmentids0	*
T0*'
_output_shapes
:���������
�
;movieId_embedding/movieId_embedding_weights/Reshape_1/shapeConst*
_output_shapes
:*
dtype0*
valueB"����   �
5movieId_embedding/movieId_embedding_weights/Reshape_1ReshapeimovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:empty_row_indicator:0DmovieId_embedding/movieId_embedding_weights/Reshape_1/shape:output:0*
T0
*'
_output_shapes
:����������
1movieId_embedding/movieId_embedding_weights/ShapeShapeLmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*
_output_shapes
::���
?movieId_embedding/movieId_embedding_weights/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB:�
AmovieId_embedding/movieId_embedding_weights/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:�
AmovieId_embedding/movieId_embedding_weights/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
9movieId_embedding/movieId_embedding_weights/strided_sliceStridedSlice:movieId_embedding/movieId_embedding_weights/Shape:output:0HmovieId_embedding/movieId_embedding_weights/strided_slice/stack:output:0JmovieId_embedding/movieId_embedding_weights/strided_slice/stack_1:output:0JmovieId_embedding/movieId_embedding_weights/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_masku
3movieId_embedding/movieId_embedding_weights/stack/0Const*
_output_shapes
: *
dtype0*
value	B :�
1movieId_embedding/movieId_embedding_weights/stackPack<movieId_embedding/movieId_embedding_weights/stack/0:output:0BmovieId_embedding/movieId_embedding_weights/strided_slice:output:0*
N*
T0*
_output_shapes
:�
0movieId_embedding/movieId_embedding_weights/TileTile>movieId_embedding/movieId_embedding_weights/Reshape_1:output:0:movieId_embedding/movieId_embedding_weights/stack:output:0*
T0
*'
_output_shapes
:���������
�
6movieId_embedding/movieId_embedding_weights/zeros_like	ZerosLikeLmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
+movieId_embedding/movieId_embedding_weightsSelect9movieId_embedding/movieId_embedding_weights/Tile:output:0:movieId_embedding/movieId_embedding_weights/zeros_like:y:0LmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
2movieId_embedding/movieId_embedding_weights/Cast_1Cast6movieId_embedding/to_sparse_input/dense_shape:output:0*

DstT0*

SrcT0	*
_output_shapes
:�
9movieId_embedding/movieId_embedding_weights/Slice_1/beginConst*
_output_shapes
:*
dtype0*
valueB: �
8movieId_embedding/movieId_embedding_weights/Slice_1/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
3movieId_embedding/movieId_embedding_weights/Slice_1Slice6movieId_embedding/movieId_embedding_weights/Cast_1:y:0BmovieId_embedding/movieId_embedding_weights/Slice_1/begin:output:0AmovieId_embedding/movieId_embedding_weights/Slice_1/size:output:0*
Index0*
T0*
_output_shapes
:�
3movieId_embedding/movieId_embedding_weights/Shape_1Shape4movieId_embedding/movieId_embedding_weights:output:0*
T0*
_output_shapes
::���
9movieId_embedding/movieId_embedding_weights/Slice_2/beginConst*
_output_shapes
:*
dtype0*
valueB:�
8movieId_embedding/movieId_embedding_weights/Slice_2/sizeConst*
_output_shapes
:*
dtype0*
valueB:
����������
3movieId_embedding/movieId_embedding_weights/Slice_2Slice<movieId_embedding/movieId_embedding_weights/Shape_1:output:0BmovieId_embedding/movieId_embedding_weights/Slice_2/begin:output:0AmovieId_embedding/movieId_embedding_weights/Slice_2/size:output:0*
Index0*
T0*
_output_shapes
:y
7movieId_embedding/movieId_embedding_weights/concat/axisConst*
_output_shapes
: *
dtype0*
value	B : �
2movieId_embedding/movieId_embedding_weights/concatConcatV2<movieId_embedding/movieId_embedding_weights/Slice_1:output:0<movieId_embedding/movieId_embedding_weights/Slice_2:output:0@movieId_embedding/movieId_embedding_weights/concat/axis:output:0*
N*
T0*
_output_shapes
:�
5movieId_embedding/movieId_embedding_weights/Reshape_2Reshape4movieId_embedding/movieId_embedding_weights:output:0;movieId_embedding/movieId_embedding_weights/concat:output:0*
T0*'
_output_shapes
:���������
�
movieId_embedding/ShapeShape>movieId_embedding/movieId_embedding_weights/Reshape_2:output:0*
T0*
_output_shapes
::��o
%movieId_embedding/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: q
'movieId_embedding/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:q
'movieId_embedding/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
movieId_embedding/strided_sliceStridedSlice movieId_embedding/Shape:output:0.movieId_embedding/strided_slice/stack:output:00movieId_embedding/strided_slice/stack_1:output:00movieId_embedding/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_maskc
!movieId_embedding/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
�
movieId_embedding/Reshape/shapePack(movieId_embedding/strided_slice:output:0*movieId_embedding/Reshape/shape/1:output:0*
N*
T0*
_output_shapes
:�
movieId_embedding/ReshapeReshape>movieId_embedding/movieId_embedding_weights/Reshape_2:output:0(movieId_embedding/Reshape/shape:output:0*
T0*'
_output_shapes
:���������
\
concat/concat_dimConst*
_output_shapes
: *
dtype0*
valueB :
���������o
concat/concatIdentity"movieId_embedding/Reshape:output:0*
T0*'
_output_shapes
:���������
e
IdentityIdentityconcat/concat:output:0^NoOp*
T0*'
_output_shapes
:���������
y
NoOpNoOpU^movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*3
_input_shapes"
 :���������:���������: 2�
TmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupTmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup:&"
 
_user_specified_name403334:TP
#
_output_shapes
:���������
)
_user_specified_namefeatures_userid:U Q
#
_output_shapes
:���������
*
_user_specified_namefeatures_movieid
�y
�
L__inference_dense_features_1_layer_call_and_return_conditional_losses_402623
features

features_1m
Yuserid_embedding_userid_embedding_weights_embedding_lookup_sparse_embedding_lookup_402584:
��

identity��RuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupj
userId_embedding/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
����������
userId_embedding/ExpandDims
ExpandDims
features_1(userId_embedding/ExpandDims/dim:output:0*
T0*'
_output_shapes
:���������z
/userId_embedding/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB :
����������
)userId_embedding/to_sparse_input/NotEqualNotEqual$userId_embedding/ExpandDims:output:08userId_embedding/to_sparse_input/ignore_value/x:output:0*
T0*'
_output_shapes
:����������
(userId_embedding/to_sparse_input/indicesWhere-userId_embedding/to_sparse_input/NotEqual:z:0*'
_output_shapes
:����������
'userId_embedding/to_sparse_input/valuesGatherNd$userId_embedding/ExpandDims:output:00userId_embedding/to_sparse_input/indices:index:0*
Tindices0	*
Tparams0*#
_output_shapes
:����������
,userId_embedding/to_sparse_input/dense_shapeShape$userId_embedding/ExpandDims:output:0*
T0*
_output_shapes
:*
out_type0	:���
userId_embedding/valuesCast0userId_embedding/to_sparse_input/values:output:0*

DstT0	*

SrcT0*#
_output_shapes
:���������
5userId_embedding/userId_embedding_weights/Slice/beginConst*
_output_shapes
:*
dtype0*
valueB: ~
4userId_embedding/userId_embedding_weights/Slice/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
/userId_embedding/userId_embedding_weights/SliceSlice5userId_embedding/to_sparse_input/dense_shape:output:0>userId_embedding/userId_embedding_weights/Slice/begin:output:0=userId_embedding/userId_embedding_weights/Slice/size:output:0*
Index0*
T0	*
_output_shapes
:y
/userId_embedding/userId_embedding_weights/ConstConst*
_output_shapes
:*
dtype0*
valueB: �
.userId_embedding/userId_embedding_weights/ProdProd8userId_embedding/userId_embedding_weights/Slice:output:08userId_embedding/userId_embedding_weights/Const:output:0*
T0	*
_output_shapes
: |
:userId_embedding/userId_embedding_weights/GatherV2/indicesConst*
_output_shapes
: *
dtype0*
value	B :y
7userId_embedding/userId_embedding_weights/GatherV2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
2userId_embedding/userId_embedding_weights/GatherV2GatherV25userId_embedding/to_sparse_input/dense_shape:output:0CuserId_embedding/userId_embedding_weights/GatherV2/indices:output:0@userId_embedding/userId_embedding_weights/GatherV2/axis:output:0*
Taxis0*
Tindices0*
Tparams0	*
_output_shapes
: �
0userId_embedding/userId_embedding_weights/Cast/xPack7userId_embedding/userId_embedding_weights/Prod:output:0;userId_embedding/userId_embedding_weights/GatherV2:output:0*
N*
T0	*
_output_shapes
:�
7userId_embedding/userId_embedding_weights/SparseReshapeSparseReshape0userId_embedding/to_sparse_input/indices:index:05userId_embedding/to_sparse_input/dense_shape:output:09userId_embedding/userId_embedding_weights/Cast/x:output:0*-
_output_shapes
:���������:�
@userId_embedding/userId_embedding_weights/SparseReshape/IdentityIdentityuserId_embedding/values:y:0*
T0	*#
_output_shapes
:���������z
8userId_embedding/userId_embedding_weights/GreaterEqual/yConst*
_output_shapes
: *
dtype0	*
value	B	 R �
6userId_embedding/userId_embedding_weights/GreaterEqualGreaterEqualIuserId_embedding/userId_embedding_weights/SparseReshape/Identity:output:0AuserId_embedding/userId_embedding_weights/GreaterEqual/y:output:0*
T0	*#
_output_shapes
:����������
/userId_embedding/userId_embedding_weights/WhereWhere:userId_embedding/userId_embedding_weights/GreaterEqual:z:0*'
_output_shapes
:����������
7userId_embedding/userId_embedding_weights/Reshape/shapeConst*
_output_shapes
:*
dtype0*
valueB:
����������
1userId_embedding/userId_embedding_weights/ReshapeReshape7userId_embedding/userId_embedding_weights/Where:index:0@userId_embedding/userId_embedding_weights/Reshape/shape:output:0*
T0	*#
_output_shapes
:���������{
9userId_embedding/userId_embedding_weights/GatherV2_1/axisConst*
_output_shapes
: *
dtype0*
value	B : �
4userId_embedding/userId_embedding_weights/GatherV2_1GatherV2HuserId_embedding/userId_embedding_weights/SparseReshape:output_indices:0:userId_embedding/userId_embedding_weights/Reshape:output:0BuserId_embedding/userId_embedding_weights/GatherV2_1/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*'
_output_shapes
:���������{
9userId_embedding/userId_embedding_weights/GatherV2_2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
4userId_embedding/userId_embedding_weights/GatherV2_2GatherV2IuserId_embedding/userId_embedding_weights/SparseReshape/Identity:output:0:userId_embedding/userId_embedding_weights/Reshape:output:0BuserId_embedding/userId_embedding_weights/GatherV2_2/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*#
_output_shapes
:����������
2userId_embedding/userId_embedding_weights/IdentityIdentityFuserId_embedding/userId_embedding_weights/SparseReshape:output_shape:0*
T0	*
_output_shapes
:�
CuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/ConstConst*
_output_shapes
: *
dtype0	*
value	B	 R �
QuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRowsSparseFillEmptyRows=userId_embedding/userId_embedding_weights/GatherV2_1:output:0=userId_embedding/userId_embedding_weights/GatherV2_2:output:0;userId_embedding/userId_embedding_weights/Identity:output:0LuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/Const:output:0*
T0	*T
_output_shapesB
@:���������:���������:���������:����������
UuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB"        �
WuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB"       �
WuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB"      �
OuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_sliceStridedSlicebuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_indices:0^userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack:output:0`userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1:output:0`userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2:output:0*
Index0*
T0	*#
_output_shapes
:���������*

begin_mask*
end_mask*
shrink_axis_mask�
HuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/UniqueUniqueauserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_values:0*
T0	*2
_output_shapes 
:���������:����������
RuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupResourceGatherYuserid_embedding_userid_embedding_weights_embedding_lookup_sparse_embedding_lookup_402584LuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/Unique:y:0*
Tindices0	*l
_classb
`^loc:@userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/402584*'
_output_shapes
:���������
*
dtype0�
[userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/IdentityIdentity[userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup:output:0*
T0*'
_output_shapes
:���������
�
AuserId_embedding/userId_embedding_weights/embedding_lookup_sparseSparseSegmentMeanduserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/Identity:output:0NuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/Unique:idx:0XuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice:output:0*
Tsegmentids0	*
T0*'
_output_shapes
:���������
�
9userId_embedding/userId_embedding_weights/Reshape_1/shapeConst*
_output_shapes
:*
dtype0*
valueB"����   �
3userId_embedding/userId_embedding_weights/Reshape_1ReshapeguserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:empty_row_indicator:0BuserId_embedding/userId_embedding_weights/Reshape_1/shape:output:0*
T0
*'
_output_shapes
:����������
/userId_embedding/userId_embedding_weights/ShapeShapeJuserId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*
_output_shapes
::���
=userId_embedding/userId_embedding_weights/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB:�
?userId_embedding/userId_embedding_weights/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:�
?userId_embedding/userId_embedding_weights/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
7userId_embedding/userId_embedding_weights/strided_sliceStridedSlice8userId_embedding/userId_embedding_weights/Shape:output:0FuserId_embedding/userId_embedding_weights/strided_slice/stack:output:0HuserId_embedding/userId_embedding_weights/strided_slice/stack_1:output:0HuserId_embedding/userId_embedding_weights/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_masks
1userId_embedding/userId_embedding_weights/stack/0Const*
_output_shapes
: *
dtype0*
value	B :�
/userId_embedding/userId_embedding_weights/stackPack:userId_embedding/userId_embedding_weights/stack/0:output:0@userId_embedding/userId_embedding_weights/strided_slice:output:0*
N*
T0*
_output_shapes
:�
.userId_embedding/userId_embedding_weights/TileTile<userId_embedding/userId_embedding_weights/Reshape_1:output:08userId_embedding/userId_embedding_weights/stack:output:0*
T0
*'
_output_shapes
:���������
�
4userId_embedding/userId_embedding_weights/zeros_like	ZerosLikeJuserId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
)userId_embedding/userId_embedding_weightsSelect7userId_embedding/userId_embedding_weights/Tile:output:08userId_embedding/userId_embedding_weights/zeros_like:y:0JuserId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
0userId_embedding/userId_embedding_weights/Cast_1Cast5userId_embedding/to_sparse_input/dense_shape:output:0*

DstT0*

SrcT0	*
_output_shapes
:�
7userId_embedding/userId_embedding_weights/Slice_1/beginConst*
_output_shapes
:*
dtype0*
valueB: �
6userId_embedding/userId_embedding_weights/Slice_1/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
1userId_embedding/userId_embedding_weights/Slice_1Slice4userId_embedding/userId_embedding_weights/Cast_1:y:0@userId_embedding/userId_embedding_weights/Slice_1/begin:output:0?userId_embedding/userId_embedding_weights/Slice_1/size:output:0*
Index0*
T0*
_output_shapes
:�
1userId_embedding/userId_embedding_weights/Shape_1Shape2userId_embedding/userId_embedding_weights:output:0*
T0*
_output_shapes
::���
7userId_embedding/userId_embedding_weights/Slice_2/beginConst*
_output_shapes
:*
dtype0*
valueB:�
6userId_embedding/userId_embedding_weights/Slice_2/sizeConst*
_output_shapes
:*
dtype0*
valueB:
����������
1userId_embedding/userId_embedding_weights/Slice_2Slice:userId_embedding/userId_embedding_weights/Shape_1:output:0@userId_embedding/userId_embedding_weights/Slice_2/begin:output:0?userId_embedding/userId_embedding_weights/Slice_2/size:output:0*
Index0*
T0*
_output_shapes
:w
5userId_embedding/userId_embedding_weights/concat/axisConst*
_output_shapes
: *
dtype0*
value	B : �
0userId_embedding/userId_embedding_weights/concatConcatV2:userId_embedding/userId_embedding_weights/Slice_1:output:0:userId_embedding/userId_embedding_weights/Slice_2:output:0>userId_embedding/userId_embedding_weights/concat/axis:output:0*
N*
T0*
_output_shapes
:�
3userId_embedding/userId_embedding_weights/Reshape_2Reshape2userId_embedding/userId_embedding_weights:output:09userId_embedding/userId_embedding_weights/concat:output:0*
T0*'
_output_shapes
:���������
�
userId_embedding/ShapeShape<userId_embedding/userId_embedding_weights/Reshape_2:output:0*
T0*
_output_shapes
::��n
$userId_embedding/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: p
&userId_embedding/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:p
&userId_embedding/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
userId_embedding/strided_sliceStridedSliceuserId_embedding/Shape:output:0-userId_embedding/strided_slice/stack:output:0/userId_embedding/strided_slice/stack_1:output:0/userId_embedding/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_maskb
 userId_embedding/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
�
userId_embedding/Reshape/shapePack'userId_embedding/strided_slice:output:0)userId_embedding/Reshape/shape/1:output:0*
N*
T0*
_output_shapes
:�
userId_embedding/ReshapeReshape<userId_embedding/userId_embedding_weights/Reshape_2:output:0'userId_embedding/Reshape/shape:output:0*
T0*'
_output_shapes
:���������
\
concat/concat_dimConst*
_output_shapes
: *
dtype0*
valueB :
���������n
concat/concatIdentity!userId_embedding/Reshape:output:0*
T0*'
_output_shapes
:���������
e
IdentityIdentityconcat/concat:output:0^NoOp*
T0*'
_output_shapes
:���������
w
NoOpNoOpS^userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*3
_input_shapes"
 :���������:���������: 2�
RuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupRuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup:&"
 
_user_specified_name402584:MI
#
_output_shapes
:���������
"
_user_specified_name
features:M I
#
_output_shapes
:���������
"
_user_specified_name
features
�
�
&__inference_model_layer_call_fn_403093
movieid

userid
unknown:
��

	unknown_0:
��

	unknown_1:
 
	unknown_2: 
	unknown_3:
 
	unknown_4: 
	unknown_5: @
	unknown_6:@
	unknown_7: @
	unknown_8:@
	unknown_9:	@�

unknown_10:	�

unknown_11:	@�

unknown_12:	�

unknown_13:

unknown_14:
identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallmovieiduseridunknown	unknown_0	unknown_1	unknown_2	unknown_3	unknown_4	unknown_5	unknown_6	unknown_7	unknown_8	unknown_9
unknown_10
unknown_11
unknown_12
unknown_13
unknown_14*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������*2
_read_only_resource_inputs
	
*-
config_proto

CPU

GPU 2J 8� *J
fERC
A__inference_model_layer_call_and_return_conditional_losses_402840o
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:���������<
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*Q
_input_shapes@
>:���������:���������: : : : : : : : : : : : : : : : 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403089:&"
 
_user_specified_name403087:&"
 
_user_specified_name403085:&"
 
_user_specified_name403083:&"
 
_user_specified_name403081:&"
 
_user_specified_name403079:&"
 
_user_specified_name403077:&
"
 
_user_specified_name403075:&	"
 
_user_specified_name403073:&"
 
_user_specified_name403071:&"
 
_user_specified_name403069:&"
 
_user_specified_name403067:&"
 
_user_specified_name403065:&"
 
_user_specified_name403063:&"
 
_user_specified_name403061:&"
 
_user_specified_name403059:KG
#
_output_shapes
:���������
 
_user_specified_nameuserId:L H
#
_output_shapes
:���������
!
_user_specified_name	movieId
��
�9
__inference__traced_save_404194
file_prefix]
Iread_disablecopyonread_dense_features_movieid_embedding_embedding_weights:
��
`
Lread_1_disablecopyonread_dense_features_1_userid_embedding_embedding_weights:
��
7
%read_2_disablecopyonread_dense_kernel:
 1
#read_3_disablecopyonread_dense_bias: 9
'read_4_disablecopyonread_dense_1_kernel:
 3
%read_5_disablecopyonread_dense_1_bias: 9
'read_6_disablecopyonread_dense_2_kernel: @3
%read_7_disablecopyonread_dense_2_bias:@9
'read_8_disablecopyonread_dense_3_kernel: @3
%read_9_disablecopyonread_dense_3_bias:@;
(read_10_disablecopyonread_dense_4_kernel:	@�5
&read_11_disablecopyonread_dense_4_bias:	�;
(read_12_disablecopyonread_dense_5_kernel:	@�5
&read_13_disablecopyonread_dense_5_bias:	�:
(read_14_disablecopyonread_dense_6_kernel:4
&read_15_disablecopyonread_dense_6_bias:-
#read_16_disablecopyonread_iteration:	 1
'read_17_disablecopyonread_learning_rate: g
Sread_18_disablecopyonread_adam_m_dense_features_movieid_embedding_embedding_weights:
��
g
Sread_19_disablecopyonread_adam_v_dense_features_movieid_embedding_embedding_weights:
��
h
Tread_20_disablecopyonread_adam_m_dense_features_1_userid_embedding_embedding_weights:
��
h
Tread_21_disablecopyonread_adam_v_dense_features_1_userid_embedding_embedding_weights:
��
?
-read_22_disablecopyonread_adam_m_dense_kernel:
 ?
-read_23_disablecopyonread_adam_v_dense_kernel:
 9
+read_24_disablecopyonread_adam_m_dense_bias: 9
+read_25_disablecopyonread_adam_v_dense_bias: A
/read_26_disablecopyonread_adam_m_dense_1_kernel:
 A
/read_27_disablecopyonread_adam_v_dense_1_kernel:
 ;
-read_28_disablecopyonread_adam_m_dense_1_bias: ;
-read_29_disablecopyonread_adam_v_dense_1_bias: A
/read_30_disablecopyonread_adam_m_dense_2_kernel: @A
/read_31_disablecopyonread_adam_v_dense_2_kernel: @;
-read_32_disablecopyonread_adam_m_dense_2_bias:@;
-read_33_disablecopyonread_adam_v_dense_2_bias:@A
/read_34_disablecopyonread_adam_m_dense_3_kernel: @A
/read_35_disablecopyonread_adam_v_dense_3_kernel: @;
-read_36_disablecopyonread_adam_m_dense_3_bias:@;
-read_37_disablecopyonread_adam_v_dense_3_bias:@B
/read_38_disablecopyonread_adam_m_dense_4_kernel:	@�B
/read_39_disablecopyonread_adam_v_dense_4_kernel:	@�<
-read_40_disablecopyonread_adam_m_dense_4_bias:	�<
-read_41_disablecopyonread_adam_v_dense_4_bias:	�B
/read_42_disablecopyonread_adam_m_dense_5_kernel:	@�B
/read_43_disablecopyonread_adam_v_dense_5_kernel:	@�<
-read_44_disablecopyonread_adam_m_dense_5_bias:	�<
-read_45_disablecopyonread_adam_v_dense_5_bias:	�A
/read_46_disablecopyonread_adam_m_dense_6_kernel:A
/read_47_disablecopyonread_adam_v_dense_6_kernel:;
-read_48_disablecopyonread_adam_m_dense_6_bias:;
-read_49_disablecopyonread_adam_v_dense_6_bias:+
!read_50_disablecopyonread_total_1: +
!read_51_disablecopyonread_count_1: )
read_52_disablecopyonread_total: )
read_53_disablecopyonread_count: 9
*read_54_disablecopyonread_true_positives_1:	�9
*read_55_disablecopyonread_true_negatives_1:	�:
+read_56_disablecopyonread_false_positives_1:	�:
+read_57_disablecopyonread_false_negatives_1:	�7
(read_58_disablecopyonread_true_positives:	�7
(read_59_disablecopyonread_true_negatives:	�8
)read_60_disablecopyonread_false_positives:	�8
)read_61_disablecopyonread_false_negatives:	�
savev2_const
identity_125��MergeV2Checkpoints�Read/DisableCopyOnRead�Read/ReadVariableOp�Read_1/DisableCopyOnRead�Read_1/ReadVariableOp�Read_10/DisableCopyOnRead�Read_10/ReadVariableOp�Read_11/DisableCopyOnRead�Read_11/ReadVariableOp�Read_12/DisableCopyOnRead�Read_12/ReadVariableOp�Read_13/DisableCopyOnRead�Read_13/ReadVariableOp�Read_14/DisableCopyOnRead�Read_14/ReadVariableOp�Read_15/DisableCopyOnRead�Read_15/ReadVariableOp�Read_16/DisableCopyOnRead�Read_16/ReadVariableOp�Read_17/DisableCopyOnRead�Read_17/ReadVariableOp�Read_18/DisableCopyOnRead�Read_18/ReadVariableOp�Read_19/DisableCopyOnRead�Read_19/ReadVariableOp�Read_2/DisableCopyOnRead�Read_2/ReadVariableOp�Read_20/DisableCopyOnRead�Read_20/ReadVariableOp�Read_21/DisableCopyOnRead�Read_21/ReadVariableOp�Read_22/DisableCopyOnRead�Read_22/ReadVariableOp�Read_23/DisableCopyOnRead�Read_23/ReadVariableOp�Read_24/DisableCopyOnRead�Read_24/ReadVariableOp�Read_25/DisableCopyOnRead�Read_25/ReadVariableOp�Read_26/DisableCopyOnRead�Read_26/ReadVariableOp�Read_27/DisableCopyOnRead�Read_27/ReadVariableOp�Read_28/DisableCopyOnRead�Read_28/ReadVariableOp�Read_29/DisableCopyOnRead�Read_29/ReadVariableOp�Read_3/DisableCopyOnRead�Read_3/ReadVariableOp�Read_30/DisableCopyOnRead�Read_30/ReadVariableOp�Read_31/DisableCopyOnRead�Read_31/ReadVariableOp�Read_32/DisableCopyOnRead�Read_32/ReadVariableOp�Read_33/DisableCopyOnRead�Read_33/ReadVariableOp�Read_34/DisableCopyOnRead�Read_34/ReadVariableOp�Read_35/DisableCopyOnRead�Read_35/ReadVariableOp�Read_36/DisableCopyOnRead�Read_36/ReadVariableOp�Read_37/DisableCopyOnRead�Read_37/ReadVariableOp�Read_38/DisableCopyOnRead�Read_38/ReadVariableOp�Read_39/DisableCopyOnRead�Read_39/ReadVariableOp�Read_4/DisableCopyOnRead�Read_4/ReadVariableOp�Read_40/DisableCopyOnRead�Read_40/ReadVariableOp�Read_41/DisableCopyOnRead�Read_41/ReadVariableOp�Read_42/DisableCopyOnRead�Read_42/ReadVariableOp�Read_43/DisableCopyOnRead�Read_43/ReadVariableOp�Read_44/DisableCopyOnRead�Read_44/ReadVariableOp�Read_45/DisableCopyOnRead�Read_45/ReadVariableOp�Read_46/DisableCopyOnRead�Read_46/ReadVariableOp�Read_47/DisableCopyOnRead�Read_47/ReadVariableOp�Read_48/DisableCopyOnRead�Read_48/ReadVariableOp�Read_49/DisableCopyOnRead�Read_49/ReadVariableOp�Read_5/DisableCopyOnRead�Read_5/ReadVariableOp�Read_50/DisableCopyOnRead�Read_50/ReadVariableOp�Read_51/DisableCopyOnRead�Read_51/ReadVariableOp�Read_52/DisableCopyOnRead�Read_52/ReadVariableOp�Read_53/DisableCopyOnRead�Read_53/ReadVariableOp�Read_54/DisableCopyOnRead�Read_54/ReadVariableOp�Read_55/DisableCopyOnRead�Read_55/ReadVariableOp�Read_56/DisableCopyOnRead�Read_56/ReadVariableOp�Read_57/DisableCopyOnRead�Read_57/ReadVariableOp�Read_58/DisableCopyOnRead�Read_58/ReadVariableOp�Read_59/DisableCopyOnRead�Read_59/ReadVariableOp�Read_6/DisableCopyOnRead�Read_6/ReadVariableOp�Read_60/DisableCopyOnRead�Read_60/ReadVariableOp�Read_61/DisableCopyOnRead�Read_61/ReadVariableOp�Read_7/DisableCopyOnRead�Read_7/ReadVariableOp�Read_8/DisableCopyOnRead�Read_8/ReadVariableOp�Read_9/DisableCopyOnRead�Read_9/ReadVariableOpw
StaticRegexFullMatchStaticRegexFullMatchfile_prefix"/device:CPU:**
_output_shapes
: *
pattern
^s3://.*Z
ConstConst"/device:CPU:**
_output_shapes
: *
dtype0*
valueB B.parta
Const_1Const"/device:CPU:**
_output_shapes
: *
dtype0*
valueB B
_temp/part�
SelectSelectStaticRegexFullMatch:output:0Const:output:0Const_1:output:0"/device:CPU:**
T0*
_output_shapes
: f

StringJoin
StringJoinfile_prefixSelect:output:0"/device:CPU:**
N*
_output_shapes
: L

num_shardsConst*
_output_shapes
: *
dtype0*
value	B :f
ShardedFilename/shardConst"/device:CPU:0*
_output_shapes
: *
dtype0*
value	B : �
ShardedFilenameShardedFilenameStringJoin:output:0ShardedFilename/shard:output:0num_shards:output:0"/device:CPU:0*
_output_shapes
: �
Read/DisableCopyOnReadDisableCopyOnReadIread_disablecopyonread_dense_features_movieid_embedding_embedding_weights"/device:CPU:0*
_output_shapes
 �
Read/ReadVariableOpReadVariableOpIread_disablecopyonread_dense_features_movieid_embedding_embedding_weights^Read/DisableCopyOnRead"/device:CPU:0* 
_output_shapes
:
��
*
dtype0k
IdentityIdentityRead/ReadVariableOp:value:0"/device:CPU:0*
T0* 
_output_shapes
:
��
c

Identity_1IdentityIdentity:output:0"/device:CPU:0*
T0* 
_output_shapes
:
��
�
Read_1/DisableCopyOnReadDisableCopyOnReadLread_1_disablecopyonread_dense_features_1_userid_embedding_embedding_weights"/device:CPU:0*
_output_shapes
 �
Read_1/ReadVariableOpReadVariableOpLread_1_disablecopyonread_dense_features_1_userid_embedding_embedding_weights^Read_1/DisableCopyOnRead"/device:CPU:0* 
_output_shapes
:
��
*
dtype0o

Identity_2IdentityRead_1/ReadVariableOp:value:0"/device:CPU:0*
T0* 
_output_shapes
:
��
e

Identity_3IdentityIdentity_2:output:0"/device:CPU:0*
T0* 
_output_shapes
:
��
y
Read_2/DisableCopyOnReadDisableCopyOnRead%read_2_disablecopyonread_dense_kernel"/device:CPU:0*
_output_shapes
 �
Read_2/ReadVariableOpReadVariableOp%read_2_disablecopyonread_dense_kernel^Read_2/DisableCopyOnRead"/device:CPU:0*
_output_shapes

:
 *
dtype0m

Identity_4IdentityRead_2/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

:
 c

Identity_5IdentityIdentity_4:output:0"/device:CPU:0*
T0*
_output_shapes

:
 w
Read_3/DisableCopyOnReadDisableCopyOnRead#read_3_disablecopyonread_dense_bias"/device:CPU:0*
_output_shapes
 �
Read_3/ReadVariableOpReadVariableOp#read_3_disablecopyonread_dense_bias^Read_3/DisableCopyOnRead"/device:CPU:0*
_output_shapes
: *
dtype0i

Identity_6IdentityRead_3/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
: _

Identity_7IdentityIdentity_6:output:0"/device:CPU:0*
T0*
_output_shapes
: {
Read_4/DisableCopyOnReadDisableCopyOnRead'read_4_disablecopyonread_dense_1_kernel"/device:CPU:0*
_output_shapes
 �
Read_4/ReadVariableOpReadVariableOp'read_4_disablecopyonread_dense_1_kernel^Read_4/DisableCopyOnRead"/device:CPU:0*
_output_shapes

:
 *
dtype0m

Identity_8IdentityRead_4/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

:
 c

Identity_9IdentityIdentity_8:output:0"/device:CPU:0*
T0*
_output_shapes

:
 y
Read_5/DisableCopyOnReadDisableCopyOnRead%read_5_disablecopyonread_dense_1_bias"/device:CPU:0*
_output_shapes
 �
Read_5/ReadVariableOpReadVariableOp%read_5_disablecopyonread_dense_1_bias^Read_5/DisableCopyOnRead"/device:CPU:0*
_output_shapes
: *
dtype0j
Identity_10IdentityRead_5/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
: a
Identity_11IdentityIdentity_10:output:0"/device:CPU:0*
T0*
_output_shapes
: {
Read_6/DisableCopyOnReadDisableCopyOnRead'read_6_disablecopyonread_dense_2_kernel"/device:CPU:0*
_output_shapes
 �
Read_6/ReadVariableOpReadVariableOp'read_6_disablecopyonread_dense_2_kernel^Read_6/DisableCopyOnRead"/device:CPU:0*
_output_shapes

: @*
dtype0n
Identity_12IdentityRead_6/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

: @e
Identity_13IdentityIdentity_12:output:0"/device:CPU:0*
T0*
_output_shapes

: @y
Read_7/DisableCopyOnReadDisableCopyOnRead%read_7_disablecopyonread_dense_2_bias"/device:CPU:0*
_output_shapes
 �
Read_7/ReadVariableOpReadVariableOp%read_7_disablecopyonread_dense_2_bias^Read_7/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:@*
dtype0j
Identity_14IdentityRead_7/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:@a
Identity_15IdentityIdentity_14:output:0"/device:CPU:0*
T0*
_output_shapes
:@{
Read_8/DisableCopyOnReadDisableCopyOnRead'read_8_disablecopyonread_dense_3_kernel"/device:CPU:0*
_output_shapes
 �
Read_8/ReadVariableOpReadVariableOp'read_8_disablecopyonread_dense_3_kernel^Read_8/DisableCopyOnRead"/device:CPU:0*
_output_shapes

: @*
dtype0n
Identity_16IdentityRead_8/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

: @e
Identity_17IdentityIdentity_16:output:0"/device:CPU:0*
T0*
_output_shapes

: @y
Read_9/DisableCopyOnReadDisableCopyOnRead%read_9_disablecopyonread_dense_3_bias"/device:CPU:0*
_output_shapes
 �
Read_9/ReadVariableOpReadVariableOp%read_9_disablecopyonread_dense_3_bias^Read_9/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:@*
dtype0j
Identity_18IdentityRead_9/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:@a
Identity_19IdentityIdentity_18:output:0"/device:CPU:0*
T0*
_output_shapes
:@}
Read_10/DisableCopyOnReadDisableCopyOnRead(read_10_disablecopyonread_dense_4_kernel"/device:CPU:0*
_output_shapes
 �
Read_10/ReadVariableOpReadVariableOp(read_10_disablecopyonread_dense_4_kernel^Read_10/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:	@�*
dtype0p
Identity_20IdentityRead_10/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:	@�f
Identity_21IdentityIdentity_20:output:0"/device:CPU:0*
T0*
_output_shapes
:	@�{
Read_11/DisableCopyOnReadDisableCopyOnRead&read_11_disablecopyonread_dense_4_bias"/device:CPU:0*
_output_shapes
 �
Read_11/ReadVariableOpReadVariableOp&read_11_disablecopyonread_dense_4_bias^Read_11/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0l
Identity_22IdentityRead_11/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�b
Identity_23IdentityIdentity_22:output:0"/device:CPU:0*
T0*
_output_shapes	
:�}
Read_12/DisableCopyOnReadDisableCopyOnRead(read_12_disablecopyonread_dense_5_kernel"/device:CPU:0*
_output_shapes
 �
Read_12/ReadVariableOpReadVariableOp(read_12_disablecopyonread_dense_5_kernel^Read_12/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:	@�*
dtype0p
Identity_24IdentityRead_12/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:	@�f
Identity_25IdentityIdentity_24:output:0"/device:CPU:0*
T0*
_output_shapes
:	@�{
Read_13/DisableCopyOnReadDisableCopyOnRead&read_13_disablecopyonread_dense_5_bias"/device:CPU:0*
_output_shapes
 �
Read_13/ReadVariableOpReadVariableOp&read_13_disablecopyonread_dense_5_bias^Read_13/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0l
Identity_26IdentityRead_13/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�b
Identity_27IdentityIdentity_26:output:0"/device:CPU:0*
T0*
_output_shapes	
:�}
Read_14/DisableCopyOnReadDisableCopyOnRead(read_14_disablecopyonread_dense_6_kernel"/device:CPU:0*
_output_shapes
 �
Read_14/ReadVariableOpReadVariableOp(read_14_disablecopyonread_dense_6_kernel^Read_14/DisableCopyOnRead"/device:CPU:0*
_output_shapes

:*
dtype0o
Identity_28IdentityRead_14/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

:e
Identity_29IdentityIdentity_28:output:0"/device:CPU:0*
T0*
_output_shapes

:{
Read_15/DisableCopyOnReadDisableCopyOnRead&read_15_disablecopyonread_dense_6_bias"/device:CPU:0*
_output_shapes
 �
Read_15/ReadVariableOpReadVariableOp&read_15_disablecopyonread_dense_6_bias^Read_15/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:*
dtype0k
Identity_30IdentityRead_15/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:a
Identity_31IdentityIdentity_30:output:0"/device:CPU:0*
T0*
_output_shapes
:x
Read_16/DisableCopyOnReadDisableCopyOnRead#read_16_disablecopyonread_iteration"/device:CPU:0*
_output_shapes
 �
Read_16/ReadVariableOpReadVariableOp#read_16_disablecopyonread_iteration^Read_16/DisableCopyOnRead"/device:CPU:0*
_output_shapes
: *
dtype0	g
Identity_32IdentityRead_16/ReadVariableOp:value:0"/device:CPU:0*
T0	*
_output_shapes
: ]
Identity_33IdentityIdentity_32:output:0"/device:CPU:0*
T0	*
_output_shapes
: |
Read_17/DisableCopyOnReadDisableCopyOnRead'read_17_disablecopyonread_learning_rate"/device:CPU:0*
_output_shapes
 �
Read_17/ReadVariableOpReadVariableOp'read_17_disablecopyonread_learning_rate^Read_17/DisableCopyOnRead"/device:CPU:0*
_output_shapes
: *
dtype0g
Identity_34IdentityRead_17/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
: ]
Identity_35IdentityIdentity_34:output:0"/device:CPU:0*
T0*
_output_shapes
: �
Read_18/DisableCopyOnReadDisableCopyOnReadSread_18_disablecopyonread_adam_m_dense_features_movieid_embedding_embedding_weights"/device:CPU:0*
_output_shapes
 �
Read_18/ReadVariableOpReadVariableOpSread_18_disablecopyonread_adam_m_dense_features_movieid_embedding_embedding_weights^Read_18/DisableCopyOnRead"/device:CPU:0* 
_output_shapes
:
��
*
dtype0q
Identity_36IdentityRead_18/ReadVariableOp:value:0"/device:CPU:0*
T0* 
_output_shapes
:
��
g
Identity_37IdentityIdentity_36:output:0"/device:CPU:0*
T0* 
_output_shapes
:
��
�
Read_19/DisableCopyOnReadDisableCopyOnReadSread_19_disablecopyonread_adam_v_dense_features_movieid_embedding_embedding_weights"/device:CPU:0*
_output_shapes
 �
Read_19/ReadVariableOpReadVariableOpSread_19_disablecopyonread_adam_v_dense_features_movieid_embedding_embedding_weights^Read_19/DisableCopyOnRead"/device:CPU:0* 
_output_shapes
:
��
*
dtype0q
Identity_38IdentityRead_19/ReadVariableOp:value:0"/device:CPU:0*
T0* 
_output_shapes
:
��
g
Identity_39IdentityIdentity_38:output:0"/device:CPU:0*
T0* 
_output_shapes
:
��
�
Read_20/DisableCopyOnReadDisableCopyOnReadTread_20_disablecopyonread_adam_m_dense_features_1_userid_embedding_embedding_weights"/device:CPU:0*
_output_shapes
 �
Read_20/ReadVariableOpReadVariableOpTread_20_disablecopyonread_adam_m_dense_features_1_userid_embedding_embedding_weights^Read_20/DisableCopyOnRead"/device:CPU:0* 
_output_shapes
:
��
*
dtype0q
Identity_40IdentityRead_20/ReadVariableOp:value:0"/device:CPU:0*
T0* 
_output_shapes
:
��
g
Identity_41IdentityIdentity_40:output:0"/device:CPU:0*
T0* 
_output_shapes
:
��
�
Read_21/DisableCopyOnReadDisableCopyOnReadTread_21_disablecopyonread_adam_v_dense_features_1_userid_embedding_embedding_weights"/device:CPU:0*
_output_shapes
 �
Read_21/ReadVariableOpReadVariableOpTread_21_disablecopyonread_adam_v_dense_features_1_userid_embedding_embedding_weights^Read_21/DisableCopyOnRead"/device:CPU:0* 
_output_shapes
:
��
*
dtype0q
Identity_42IdentityRead_21/ReadVariableOp:value:0"/device:CPU:0*
T0* 
_output_shapes
:
��
g
Identity_43IdentityIdentity_42:output:0"/device:CPU:0*
T0* 
_output_shapes
:
��
�
Read_22/DisableCopyOnReadDisableCopyOnRead-read_22_disablecopyonread_adam_m_dense_kernel"/device:CPU:0*
_output_shapes
 �
Read_22/ReadVariableOpReadVariableOp-read_22_disablecopyonread_adam_m_dense_kernel^Read_22/DisableCopyOnRead"/device:CPU:0*
_output_shapes

:
 *
dtype0o
Identity_44IdentityRead_22/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

:
 e
Identity_45IdentityIdentity_44:output:0"/device:CPU:0*
T0*
_output_shapes

:
 �
Read_23/DisableCopyOnReadDisableCopyOnRead-read_23_disablecopyonread_adam_v_dense_kernel"/device:CPU:0*
_output_shapes
 �
Read_23/ReadVariableOpReadVariableOp-read_23_disablecopyonread_adam_v_dense_kernel^Read_23/DisableCopyOnRead"/device:CPU:0*
_output_shapes

:
 *
dtype0o
Identity_46IdentityRead_23/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

:
 e
Identity_47IdentityIdentity_46:output:0"/device:CPU:0*
T0*
_output_shapes

:
 �
Read_24/DisableCopyOnReadDisableCopyOnRead+read_24_disablecopyonread_adam_m_dense_bias"/device:CPU:0*
_output_shapes
 �
Read_24/ReadVariableOpReadVariableOp+read_24_disablecopyonread_adam_m_dense_bias^Read_24/DisableCopyOnRead"/device:CPU:0*
_output_shapes
: *
dtype0k
Identity_48IdentityRead_24/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
: a
Identity_49IdentityIdentity_48:output:0"/device:CPU:0*
T0*
_output_shapes
: �
Read_25/DisableCopyOnReadDisableCopyOnRead+read_25_disablecopyonread_adam_v_dense_bias"/device:CPU:0*
_output_shapes
 �
Read_25/ReadVariableOpReadVariableOp+read_25_disablecopyonread_adam_v_dense_bias^Read_25/DisableCopyOnRead"/device:CPU:0*
_output_shapes
: *
dtype0k
Identity_50IdentityRead_25/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
: a
Identity_51IdentityIdentity_50:output:0"/device:CPU:0*
T0*
_output_shapes
: �
Read_26/DisableCopyOnReadDisableCopyOnRead/read_26_disablecopyonread_adam_m_dense_1_kernel"/device:CPU:0*
_output_shapes
 �
Read_26/ReadVariableOpReadVariableOp/read_26_disablecopyonread_adam_m_dense_1_kernel^Read_26/DisableCopyOnRead"/device:CPU:0*
_output_shapes

:
 *
dtype0o
Identity_52IdentityRead_26/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

:
 e
Identity_53IdentityIdentity_52:output:0"/device:CPU:0*
T0*
_output_shapes

:
 �
Read_27/DisableCopyOnReadDisableCopyOnRead/read_27_disablecopyonread_adam_v_dense_1_kernel"/device:CPU:0*
_output_shapes
 �
Read_27/ReadVariableOpReadVariableOp/read_27_disablecopyonread_adam_v_dense_1_kernel^Read_27/DisableCopyOnRead"/device:CPU:0*
_output_shapes

:
 *
dtype0o
Identity_54IdentityRead_27/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

:
 e
Identity_55IdentityIdentity_54:output:0"/device:CPU:0*
T0*
_output_shapes

:
 �
Read_28/DisableCopyOnReadDisableCopyOnRead-read_28_disablecopyonread_adam_m_dense_1_bias"/device:CPU:0*
_output_shapes
 �
Read_28/ReadVariableOpReadVariableOp-read_28_disablecopyonread_adam_m_dense_1_bias^Read_28/DisableCopyOnRead"/device:CPU:0*
_output_shapes
: *
dtype0k
Identity_56IdentityRead_28/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
: a
Identity_57IdentityIdentity_56:output:0"/device:CPU:0*
T0*
_output_shapes
: �
Read_29/DisableCopyOnReadDisableCopyOnRead-read_29_disablecopyonread_adam_v_dense_1_bias"/device:CPU:0*
_output_shapes
 �
Read_29/ReadVariableOpReadVariableOp-read_29_disablecopyonread_adam_v_dense_1_bias^Read_29/DisableCopyOnRead"/device:CPU:0*
_output_shapes
: *
dtype0k
Identity_58IdentityRead_29/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
: a
Identity_59IdentityIdentity_58:output:0"/device:CPU:0*
T0*
_output_shapes
: �
Read_30/DisableCopyOnReadDisableCopyOnRead/read_30_disablecopyonread_adam_m_dense_2_kernel"/device:CPU:0*
_output_shapes
 �
Read_30/ReadVariableOpReadVariableOp/read_30_disablecopyonread_adam_m_dense_2_kernel^Read_30/DisableCopyOnRead"/device:CPU:0*
_output_shapes

: @*
dtype0o
Identity_60IdentityRead_30/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

: @e
Identity_61IdentityIdentity_60:output:0"/device:CPU:0*
T0*
_output_shapes

: @�
Read_31/DisableCopyOnReadDisableCopyOnRead/read_31_disablecopyonread_adam_v_dense_2_kernel"/device:CPU:0*
_output_shapes
 �
Read_31/ReadVariableOpReadVariableOp/read_31_disablecopyonread_adam_v_dense_2_kernel^Read_31/DisableCopyOnRead"/device:CPU:0*
_output_shapes

: @*
dtype0o
Identity_62IdentityRead_31/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

: @e
Identity_63IdentityIdentity_62:output:0"/device:CPU:0*
T0*
_output_shapes

: @�
Read_32/DisableCopyOnReadDisableCopyOnRead-read_32_disablecopyonread_adam_m_dense_2_bias"/device:CPU:0*
_output_shapes
 �
Read_32/ReadVariableOpReadVariableOp-read_32_disablecopyonread_adam_m_dense_2_bias^Read_32/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:@*
dtype0k
Identity_64IdentityRead_32/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:@a
Identity_65IdentityIdentity_64:output:0"/device:CPU:0*
T0*
_output_shapes
:@�
Read_33/DisableCopyOnReadDisableCopyOnRead-read_33_disablecopyonread_adam_v_dense_2_bias"/device:CPU:0*
_output_shapes
 �
Read_33/ReadVariableOpReadVariableOp-read_33_disablecopyonread_adam_v_dense_2_bias^Read_33/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:@*
dtype0k
Identity_66IdentityRead_33/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:@a
Identity_67IdentityIdentity_66:output:0"/device:CPU:0*
T0*
_output_shapes
:@�
Read_34/DisableCopyOnReadDisableCopyOnRead/read_34_disablecopyonread_adam_m_dense_3_kernel"/device:CPU:0*
_output_shapes
 �
Read_34/ReadVariableOpReadVariableOp/read_34_disablecopyonread_adam_m_dense_3_kernel^Read_34/DisableCopyOnRead"/device:CPU:0*
_output_shapes

: @*
dtype0o
Identity_68IdentityRead_34/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

: @e
Identity_69IdentityIdentity_68:output:0"/device:CPU:0*
T0*
_output_shapes

: @�
Read_35/DisableCopyOnReadDisableCopyOnRead/read_35_disablecopyonread_adam_v_dense_3_kernel"/device:CPU:0*
_output_shapes
 �
Read_35/ReadVariableOpReadVariableOp/read_35_disablecopyonread_adam_v_dense_3_kernel^Read_35/DisableCopyOnRead"/device:CPU:0*
_output_shapes

: @*
dtype0o
Identity_70IdentityRead_35/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

: @e
Identity_71IdentityIdentity_70:output:0"/device:CPU:0*
T0*
_output_shapes

: @�
Read_36/DisableCopyOnReadDisableCopyOnRead-read_36_disablecopyonread_adam_m_dense_3_bias"/device:CPU:0*
_output_shapes
 �
Read_36/ReadVariableOpReadVariableOp-read_36_disablecopyonread_adam_m_dense_3_bias^Read_36/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:@*
dtype0k
Identity_72IdentityRead_36/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:@a
Identity_73IdentityIdentity_72:output:0"/device:CPU:0*
T0*
_output_shapes
:@�
Read_37/DisableCopyOnReadDisableCopyOnRead-read_37_disablecopyonread_adam_v_dense_3_bias"/device:CPU:0*
_output_shapes
 �
Read_37/ReadVariableOpReadVariableOp-read_37_disablecopyonread_adam_v_dense_3_bias^Read_37/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:@*
dtype0k
Identity_74IdentityRead_37/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:@a
Identity_75IdentityIdentity_74:output:0"/device:CPU:0*
T0*
_output_shapes
:@�
Read_38/DisableCopyOnReadDisableCopyOnRead/read_38_disablecopyonread_adam_m_dense_4_kernel"/device:CPU:0*
_output_shapes
 �
Read_38/ReadVariableOpReadVariableOp/read_38_disablecopyonread_adam_m_dense_4_kernel^Read_38/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:	@�*
dtype0p
Identity_76IdentityRead_38/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:	@�f
Identity_77IdentityIdentity_76:output:0"/device:CPU:0*
T0*
_output_shapes
:	@��
Read_39/DisableCopyOnReadDisableCopyOnRead/read_39_disablecopyonread_adam_v_dense_4_kernel"/device:CPU:0*
_output_shapes
 �
Read_39/ReadVariableOpReadVariableOp/read_39_disablecopyonread_adam_v_dense_4_kernel^Read_39/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:	@�*
dtype0p
Identity_78IdentityRead_39/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:	@�f
Identity_79IdentityIdentity_78:output:0"/device:CPU:0*
T0*
_output_shapes
:	@��
Read_40/DisableCopyOnReadDisableCopyOnRead-read_40_disablecopyonread_adam_m_dense_4_bias"/device:CPU:0*
_output_shapes
 �
Read_40/ReadVariableOpReadVariableOp-read_40_disablecopyonread_adam_m_dense_4_bias^Read_40/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0l
Identity_80IdentityRead_40/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�b
Identity_81IdentityIdentity_80:output:0"/device:CPU:0*
T0*
_output_shapes	
:��
Read_41/DisableCopyOnReadDisableCopyOnRead-read_41_disablecopyonread_adam_v_dense_4_bias"/device:CPU:0*
_output_shapes
 �
Read_41/ReadVariableOpReadVariableOp-read_41_disablecopyonread_adam_v_dense_4_bias^Read_41/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0l
Identity_82IdentityRead_41/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�b
Identity_83IdentityIdentity_82:output:0"/device:CPU:0*
T0*
_output_shapes	
:��
Read_42/DisableCopyOnReadDisableCopyOnRead/read_42_disablecopyonread_adam_m_dense_5_kernel"/device:CPU:0*
_output_shapes
 �
Read_42/ReadVariableOpReadVariableOp/read_42_disablecopyonread_adam_m_dense_5_kernel^Read_42/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:	@�*
dtype0p
Identity_84IdentityRead_42/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:	@�f
Identity_85IdentityIdentity_84:output:0"/device:CPU:0*
T0*
_output_shapes
:	@��
Read_43/DisableCopyOnReadDisableCopyOnRead/read_43_disablecopyonread_adam_v_dense_5_kernel"/device:CPU:0*
_output_shapes
 �
Read_43/ReadVariableOpReadVariableOp/read_43_disablecopyonread_adam_v_dense_5_kernel^Read_43/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:	@�*
dtype0p
Identity_86IdentityRead_43/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:	@�f
Identity_87IdentityIdentity_86:output:0"/device:CPU:0*
T0*
_output_shapes
:	@��
Read_44/DisableCopyOnReadDisableCopyOnRead-read_44_disablecopyonread_adam_m_dense_5_bias"/device:CPU:0*
_output_shapes
 �
Read_44/ReadVariableOpReadVariableOp-read_44_disablecopyonread_adam_m_dense_5_bias^Read_44/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0l
Identity_88IdentityRead_44/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�b
Identity_89IdentityIdentity_88:output:0"/device:CPU:0*
T0*
_output_shapes	
:��
Read_45/DisableCopyOnReadDisableCopyOnRead-read_45_disablecopyonread_adam_v_dense_5_bias"/device:CPU:0*
_output_shapes
 �
Read_45/ReadVariableOpReadVariableOp-read_45_disablecopyonread_adam_v_dense_5_bias^Read_45/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0l
Identity_90IdentityRead_45/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�b
Identity_91IdentityIdentity_90:output:0"/device:CPU:0*
T0*
_output_shapes	
:��
Read_46/DisableCopyOnReadDisableCopyOnRead/read_46_disablecopyonread_adam_m_dense_6_kernel"/device:CPU:0*
_output_shapes
 �
Read_46/ReadVariableOpReadVariableOp/read_46_disablecopyonread_adam_m_dense_6_kernel^Read_46/DisableCopyOnRead"/device:CPU:0*
_output_shapes

:*
dtype0o
Identity_92IdentityRead_46/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

:e
Identity_93IdentityIdentity_92:output:0"/device:CPU:0*
T0*
_output_shapes

:�
Read_47/DisableCopyOnReadDisableCopyOnRead/read_47_disablecopyonread_adam_v_dense_6_kernel"/device:CPU:0*
_output_shapes
 �
Read_47/ReadVariableOpReadVariableOp/read_47_disablecopyonread_adam_v_dense_6_kernel^Read_47/DisableCopyOnRead"/device:CPU:0*
_output_shapes

:*
dtype0o
Identity_94IdentityRead_47/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes

:e
Identity_95IdentityIdentity_94:output:0"/device:CPU:0*
T0*
_output_shapes

:�
Read_48/DisableCopyOnReadDisableCopyOnRead-read_48_disablecopyonread_adam_m_dense_6_bias"/device:CPU:0*
_output_shapes
 �
Read_48/ReadVariableOpReadVariableOp-read_48_disablecopyonread_adam_m_dense_6_bias^Read_48/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:*
dtype0k
Identity_96IdentityRead_48/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:a
Identity_97IdentityIdentity_96:output:0"/device:CPU:0*
T0*
_output_shapes
:�
Read_49/DisableCopyOnReadDisableCopyOnRead-read_49_disablecopyonread_adam_v_dense_6_bias"/device:CPU:0*
_output_shapes
 �
Read_49/ReadVariableOpReadVariableOp-read_49_disablecopyonread_adam_v_dense_6_bias^Read_49/DisableCopyOnRead"/device:CPU:0*
_output_shapes
:*
dtype0k
Identity_98IdentityRead_49/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
:a
Identity_99IdentityIdentity_98:output:0"/device:CPU:0*
T0*
_output_shapes
:v
Read_50/DisableCopyOnReadDisableCopyOnRead!read_50_disablecopyonread_total_1"/device:CPU:0*
_output_shapes
 �
Read_50/ReadVariableOpReadVariableOp!read_50_disablecopyonread_total_1^Read_50/DisableCopyOnRead"/device:CPU:0*
_output_shapes
: *
dtype0h
Identity_100IdentityRead_50/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
: _
Identity_101IdentityIdentity_100:output:0"/device:CPU:0*
T0*
_output_shapes
: v
Read_51/DisableCopyOnReadDisableCopyOnRead!read_51_disablecopyonread_count_1"/device:CPU:0*
_output_shapes
 �
Read_51/ReadVariableOpReadVariableOp!read_51_disablecopyonread_count_1^Read_51/DisableCopyOnRead"/device:CPU:0*
_output_shapes
: *
dtype0h
Identity_102IdentityRead_51/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
: _
Identity_103IdentityIdentity_102:output:0"/device:CPU:0*
T0*
_output_shapes
: t
Read_52/DisableCopyOnReadDisableCopyOnReadread_52_disablecopyonread_total"/device:CPU:0*
_output_shapes
 �
Read_52/ReadVariableOpReadVariableOpread_52_disablecopyonread_total^Read_52/DisableCopyOnRead"/device:CPU:0*
_output_shapes
: *
dtype0h
Identity_104IdentityRead_52/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
: _
Identity_105IdentityIdentity_104:output:0"/device:CPU:0*
T0*
_output_shapes
: t
Read_53/DisableCopyOnReadDisableCopyOnReadread_53_disablecopyonread_count"/device:CPU:0*
_output_shapes
 �
Read_53/ReadVariableOpReadVariableOpread_53_disablecopyonread_count^Read_53/DisableCopyOnRead"/device:CPU:0*
_output_shapes
: *
dtype0h
Identity_106IdentityRead_53/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes
: _
Identity_107IdentityIdentity_106:output:0"/device:CPU:0*
T0*
_output_shapes
: 
Read_54/DisableCopyOnReadDisableCopyOnRead*read_54_disablecopyonread_true_positives_1"/device:CPU:0*
_output_shapes
 �
Read_54/ReadVariableOpReadVariableOp*read_54_disablecopyonread_true_positives_1^Read_54/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0m
Identity_108IdentityRead_54/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�d
Identity_109IdentityIdentity_108:output:0"/device:CPU:0*
T0*
_output_shapes	
:�
Read_55/DisableCopyOnReadDisableCopyOnRead*read_55_disablecopyonread_true_negatives_1"/device:CPU:0*
_output_shapes
 �
Read_55/ReadVariableOpReadVariableOp*read_55_disablecopyonread_true_negatives_1^Read_55/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0m
Identity_110IdentityRead_55/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�d
Identity_111IdentityIdentity_110:output:0"/device:CPU:0*
T0*
_output_shapes	
:��
Read_56/DisableCopyOnReadDisableCopyOnRead+read_56_disablecopyonread_false_positives_1"/device:CPU:0*
_output_shapes
 �
Read_56/ReadVariableOpReadVariableOp+read_56_disablecopyonread_false_positives_1^Read_56/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0m
Identity_112IdentityRead_56/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�d
Identity_113IdentityIdentity_112:output:0"/device:CPU:0*
T0*
_output_shapes	
:��
Read_57/DisableCopyOnReadDisableCopyOnRead+read_57_disablecopyonread_false_negatives_1"/device:CPU:0*
_output_shapes
 �
Read_57/ReadVariableOpReadVariableOp+read_57_disablecopyonread_false_negatives_1^Read_57/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0m
Identity_114IdentityRead_57/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�d
Identity_115IdentityIdentity_114:output:0"/device:CPU:0*
T0*
_output_shapes	
:�}
Read_58/DisableCopyOnReadDisableCopyOnRead(read_58_disablecopyonread_true_positives"/device:CPU:0*
_output_shapes
 �
Read_58/ReadVariableOpReadVariableOp(read_58_disablecopyonread_true_positives^Read_58/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0m
Identity_116IdentityRead_58/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�d
Identity_117IdentityIdentity_116:output:0"/device:CPU:0*
T0*
_output_shapes	
:�}
Read_59/DisableCopyOnReadDisableCopyOnRead(read_59_disablecopyonread_true_negatives"/device:CPU:0*
_output_shapes
 �
Read_59/ReadVariableOpReadVariableOp(read_59_disablecopyonread_true_negatives^Read_59/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0m
Identity_118IdentityRead_59/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�d
Identity_119IdentityIdentity_118:output:0"/device:CPU:0*
T0*
_output_shapes	
:�~
Read_60/DisableCopyOnReadDisableCopyOnRead)read_60_disablecopyonread_false_positives"/device:CPU:0*
_output_shapes
 �
Read_60/ReadVariableOpReadVariableOp)read_60_disablecopyonread_false_positives^Read_60/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0m
Identity_120IdentityRead_60/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�d
Identity_121IdentityIdentity_120:output:0"/device:CPU:0*
T0*
_output_shapes	
:�~
Read_61/DisableCopyOnReadDisableCopyOnRead)read_61_disablecopyonread_false_negatives"/device:CPU:0*
_output_shapes
 �
Read_61/ReadVariableOpReadVariableOp)read_61_disablecopyonread_false_negatives^Read_61/DisableCopyOnRead"/device:CPU:0*
_output_shapes	
:�*
dtype0m
Identity_122IdentityRead_61/ReadVariableOp:value:0"/device:CPU:0*
T0*
_output_shapes	
:�d
Identity_123IdentityIdentity_122:output:0"/device:CPU:0*
T0*
_output_shapes	
:��
SaveV2/tensor_namesConst"/device:CPU:0*
_output_shapes
:?*
dtype0*�
value�B�?BTlayer_with_weights-0/movieId_embedding.Sembedding_weights/.ATTRIBUTES/VARIABLE_VALUEBSlayer_with_weights-1/userId_embedding.Sembedding_weights/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-2/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-2/bias/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-3/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-3/bias/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-4/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-4/bias/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-5/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-5/bias/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-6/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-6/bias/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-7/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-7/bias/.ATTRIBUTES/VARIABLE_VALUEB6layer_with_weights-8/kernel/.ATTRIBUTES/VARIABLE_VALUEB4layer_with_weights-8/bias/.ATTRIBUTES/VARIABLE_VALUEB0optimizer/_iterations/.ATTRIBUTES/VARIABLE_VALUEB3optimizer/_learning_rate/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/1/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/2/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/3/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/4/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/5/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/6/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/7/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/8/.ATTRIBUTES/VARIABLE_VALUEB1optimizer/_variables/9/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/10/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/11/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/12/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/13/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/14/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/15/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/16/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/17/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/18/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/19/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/20/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/21/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/22/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/23/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/24/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/25/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/26/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/27/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/28/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/29/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/30/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/31/.ATTRIBUTES/VARIABLE_VALUEB2optimizer/_variables/32/.ATTRIBUTES/VARIABLE_VALUEB4keras_api/metrics/0/total/.ATTRIBUTES/VARIABLE_VALUEB4keras_api/metrics/0/count/.ATTRIBUTES/VARIABLE_VALUEB4keras_api/metrics/1/total/.ATTRIBUTES/VARIABLE_VALUEB4keras_api/metrics/1/count/.ATTRIBUTES/VARIABLE_VALUEB=keras_api/metrics/2/true_positives/.ATTRIBUTES/VARIABLE_VALUEB=keras_api/metrics/2/true_negatives/.ATTRIBUTES/VARIABLE_VALUEB>keras_api/metrics/2/false_positives/.ATTRIBUTES/VARIABLE_VALUEB>keras_api/metrics/2/false_negatives/.ATTRIBUTES/VARIABLE_VALUEB=keras_api/metrics/3/true_positives/.ATTRIBUTES/VARIABLE_VALUEB=keras_api/metrics/3/true_negatives/.ATTRIBUTES/VARIABLE_VALUEB>keras_api/metrics/3/false_positives/.ATTRIBUTES/VARIABLE_VALUEB>keras_api/metrics/3/false_negatives/.ATTRIBUTES/VARIABLE_VALUEB_CHECKPOINTABLE_OBJECT_GRAPH�
SaveV2/shape_and_slicesConst"/device:CPU:0*
_output_shapes
:?*
dtype0*�
value�B�?B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B B �
SaveV2SaveV2ShardedFilename:filename:0SaveV2/tensor_names:output:0 SaveV2/shape_and_slices:output:0Identity_1:output:0Identity_3:output:0Identity_5:output:0Identity_7:output:0Identity_9:output:0Identity_11:output:0Identity_13:output:0Identity_15:output:0Identity_17:output:0Identity_19:output:0Identity_21:output:0Identity_23:output:0Identity_25:output:0Identity_27:output:0Identity_29:output:0Identity_31:output:0Identity_33:output:0Identity_35:output:0Identity_37:output:0Identity_39:output:0Identity_41:output:0Identity_43:output:0Identity_45:output:0Identity_47:output:0Identity_49:output:0Identity_51:output:0Identity_53:output:0Identity_55:output:0Identity_57:output:0Identity_59:output:0Identity_61:output:0Identity_63:output:0Identity_65:output:0Identity_67:output:0Identity_69:output:0Identity_71:output:0Identity_73:output:0Identity_75:output:0Identity_77:output:0Identity_79:output:0Identity_81:output:0Identity_83:output:0Identity_85:output:0Identity_87:output:0Identity_89:output:0Identity_91:output:0Identity_93:output:0Identity_95:output:0Identity_97:output:0Identity_99:output:0Identity_101:output:0Identity_103:output:0Identity_105:output:0Identity_107:output:0Identity_109:output:0Identity_111:output:0Identity_113:output:0Identity_115:output:0Identity_117:output:0Identity_119:output:0Identity_121:output:0Identity_123:output:0savev2_const"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 *M
dtypesC
A2?	�
&MergeV2Checkpoints/checkpoint_prefixesPackShardedFilename:filename:0^SaveV2"/device:CPU:0*
N*
T0*
_output_shapes
:�
MergeV2CheckpointsMergeV2Checkpoints/MergeV2Checkpoints/checkpoint_prefixes:output:0file_prefix"/device:CPU:0*&
 _has_manual_control_dependencies(*
_output_shapes
 j
Identity_124Identityfile_prefix^MergeV2Checkpoints"/device:CPU:0*
T0*
_output_shapes
: W
Identity_125IdentityIdentity_124:output:0^NoOp*
T0*
_output_shapes
: �
NoOpNoOp^MergeV2Checkpoints^Read/DisableCopyOnRead^Read/ReadVariableOp^Read_1/DisableCopyOnRead^Read_1/ReadVariableOp^Read_10/DisableCopyOnRead^Read_10/ReadVariableOp^Read_11/DisableCopyOnRead^Read_11/ReadVariableOp^Read_12/DisableCopyOnRead^Read_12/ReadVariableOp^Read_13/DisableCopyOnRead^Read_13/ReadVariableOp^Read_14/DisableCopyOnRead^Read_14/ReadVariableOp^Read_15/DisableCopyOnRead^Read_15/ReadVariableOp^Read_16/DisableCopyOnRead^Read_16/ReadVariableOp^Read_17/DisableCopyOnRead^Read_17/ReadVariableOp^Read_18/DisableCopyOnRead^Read_18/ReadVariableOp^Read_19/DisableCopyOnRead^Read_19/ReadVariableOp^Read_2/DisableCopyOnRead^Read_2/ReadVariableOp^Read_20/DisableCopyOnRead^Read_20/ReadVariableOp^Read_21/DisableCopyOnRead^Read_21/ReadVariableOp^Read_22/DisableCopyOnRead^Read_22/ReadVariableOp^Read_23/DisableCopyOnRead^Read_23/ReadVariableOp^Read_24/DisableCopyOnRead^Read_24/ReadVariableOp^Read_25/DisableCopyOnRead^Read_25/ReadVariableOp^Read_26/DisableCopyOnRead^Read_26/ReadVariableOp^Read_27/DisableCopyOnRead^Read_27/ReadVariableOp^Read_28/DisableCopyOnRead^Read_28/ReadVariableOp^Read_29/DisableCopyOnRead^Read_29/ReadVariableOp^Read_3/DisableCopyOnRead^Read_3/ReadVariableOp^Read_30/DisableCopyOnRead^Read_30/ReadVariableOp^Read_31/DisableCopyOnRead^Read_31/ReadVariableOp^Read_32/DisableCopyOnRead^Read_32/ReadVariableOp^Read_33/DisableCopyOnRead^Read_33/ReadVariableOp^Read_34/DisableCopyOnRead^Read_34/ReadVariableOp^Read_35/DisableCopyOnRead^Read_35/ReadVariableOp^Read_36/DisableCopyOnRead^Read_36/ReadVariableOp^Read_37/DisableCopyOnRead^Read_37/ReadVariableOp^Read_38/DisableCopyOnRead^Read_38/ReadVariableOp^Read_39/DisableCopyOnRead^Read_39/ReadVariableOp^Read_4/DisableCopyOnRead^Read_4/ReadVariableOp^Read_40/DisableCopyOnRead^Read_40/ReadVariableOp^Read_41/DisableCopyOnRead^Read_41/ReadVariableOp^Read_42/DisableCopyOnRead^Read_42/ReadVariableOp^Read_43/DisableCopyOnRead^Read_43/ReadVariableOp^Read_44/DisableCopyOnRead^Read_44/ReadVariableOp^Read_45/DisableCopyOnRead^Read_45/ReadVariableOp^Read_46/DisableCopyOnRead^Read_46/ReadVariableOp^Read_47/DisableCopyOnRead^Read_47/ReadVariableOp^Read_48/DisableCopyOnRead^Read_48/ReadVariableOp^Read_49/DisableCopyOnRead^Read_49/ReadVariableOp^Read_5/DisableCopyOnRead^Read_5/ReadVariableOp^Read_50/DisableCopyOnRead^Read_50/ReadVariableOp^Read_51/DisableCopyOnRead^Read_51/ReadVariableOp^Read_52/DisableCopyOnRead^Read_52/ReadVariableOp^Read_53/DisableCopyOnRead^Read_53/ReadVariableOp^Read_54/DisableCopyOnRead^Read_54/ReadVariableOp^Read_55/DisableCopyOnRead^Read_55/ReadVariableOp^Read_56/DisableCopyOnRead^Read_56/ReadVariableOp^Read_57/DisableCopyOnRead^Read_57/ReadVariableOp^Read_58/DisableCopyOnRead^Read_58/ReadVariableOp^Read_59/DisableCopyOnRead^Read_59/ReadVariableOp^Read_6/DisableCopyOnRead^Read_6/ReadVariableOp^Read_60/DisableCopyOnRead^Read_60/ReadVariableOp^Read_61/DisableCopyOnRead^Read_61/ReadVariableOp^Read_7/DisableCopyOnRead^Read_7/ReadVariableOp^Read_8/DisableCopyOnRead^Read_8/ReadVariableOp^Read_9/DisableCopyOnRead^Read_9/ReadVariableOp*
_output_shapes
 "%
identity_125Identity_125:output:0*(
_construction_contextkEagerRuntime*�
_input_shapes�
�: : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : : 2(
MergeV2CheckpointsMergeV2Checkpoints20
Read/DisableCopyOnReadRead/DisableCopyOnRead2*
Read/ReadVariableOpRead/ReadVariableOp24
Read_1/DisableCopyOnReadRead_1/DisableCopyOnRead2.
Read_1/ReadVariableOpRead_1/ReadVariableOp26
Read_10/DisableCopyOnReadRead_10/DisableCopyOnRead20
Read_10/ReadVariableOpRead_10/ReadVariableOp26
Read_11/DisableCopyOnReadRead_11/DisableCopyOnRead20
Read_11/ReadVariableOpRead_11/ReadVariableOp26
Read_12/DisableCopyOnReadRead_12/DisableCopyOnRead20
Read_12/ReadVariableOpRead_12/ReadVariableOp26
Read_13/DisableCopyOnReadRead_13/DisableCopyOnRead20
Read_13/ReadVariableOpRead_13/ReadVariableOp26
Read_14/DisableCopyOnReadRead_14/DisableCopyOnRead20
Read_14/ReadVariableOpRead_14/ReadVariableOp26
Read_15/DisableCopyOnReadRead_15/DisableCopyOnRead20
Read_15/ReadVariableOpRead_15/ReadVariableOp26
Read_16/DisableCopyOnReadRead_16/DisableCopyOnRead20
Read_16/ReadVariableOpRead_16/ReadVariableOp26
Read_17/DisableCopyOnReadRead_17/DisableCopyOnRead20
Read_17/ReadVariableOpRead_17/ReadVariableOp26
Read_18/DisableCopyOnReadRead_18/DisableCopyOnRead20
Read_18/ReadVariableOpRead_18/ReadVariableOp26
Read_19/DisableCopyOnReadRead_19/DisableCopyOnRead20
Read_19/ReadVariableOpRead_19/ReadVariableOp24
Read_2/DisableCopyOnReadRead_2/DisableCopyOnRead2.
Read_2/ReadVariableOpRead_2/ReadVariableOp26
Read_20/DisableCopyOnReadRead_20/DisableCopyOnRead20
Read_20/ReadVariableOpRead_20/ReadVariableOp26
Read_21/DisableCopyOnReadRead_21/DisableCopyOnRead20
Read_21/ReadVariableOpRead_21/ReadVariableOp26
Read_22/DisableCopyOnReadRead_22/DisableCopyOnRead20
Read_22/ReadVariableOpRead_22/ReadVariableOp26
Read_23/DisableCopyOnReadRead_23/DisableCopyOnRead20
Read_23/ReadVariableOpRead_23/ReadVariableOp26
Read_24/DisableCopyOnReadRead_24/DisableCopyOnRead20
Read_24/ReadVariableOpRead_24/ReadVariableOp26
Read_25/DisableCopyOnReadRead_25/DisableCopyOnRead20
Read_25/ReadVariableOpRead_25/ReadVariableOp26
Read_26/DisableCopyOnReadRead_26/DisableCopyOnRead20
Read_26/ReadVariableOpRead_26/ReadVariableOp26
Read_27/DisableCopyOnReadRead_27/DisableCopyOnRead20
Read_27/ReadVariableOpRead_27/ReadVariableOp26
Read_28/DisableCopyOnReadRead_28/DisableCopyOnRead20
Read_28/ReadVariableOpRead_28/ReadVariableOp26
Read_29/DisableCopyOnReadRead_29/DisableCopyOnRead20
Read_29/ReadVariableOpRead_29/ReadVariableOp24
Read_3/DisableCopyOnReadRead_3/DisableCopyOnRead2.
Read_3/ReadVariableOpRead_3/ReadVariableOp26
Read_30/DisableCopyOnReadRead_30/DisableCopyOnRead20
Read_30/ReadVariableOpRead_30/ReadVariableOp26
Read_31/DisableCopyOnReadRead_31/DisableCopyOnRead20
Read_31/ReadVariableOpRead_31/ReadVariableOp26
Read_32/DisableCopyOnReadRead_32/DisableCopyOnRead20
Read_32/ReadVariableOpRead_32/ReadVariableOp26
Read_33/DisableCopyOnReadRead_33/DisableCopyOnRead20
Read_33/ReadVariableOpRead_33/ReadVariableOp26
Read_34/DisableCopyOnReadRead_34/DisableCopyOnRead20
Read_34/ReadVariableOpRead_34/ReadVariableOp26
Read_35/DisableCopyOnReadRead_35/DisableCopyOnRead20
Read_35/ReadVariableOpRead_35/ReadVariableOp26
Read_36/DisableCopyOnReadRead_36/DisableCopyOnRead20
Read_36/ReadVariableOpRead_36/ReadVariableOp26
Read_37/DisableCopyOnReadRead_37/DisableCopyOnRead20
Read_37/ReadVariableOpRead_37/ReadVariableOp26
Read_38/DisableCopyOnReadRead_38/DisableCopyOnRead20
Read_38/ReadVariableOpRead_38/ReadVariableOp26
Read_39/DisableCopyOnReadRead_39/DisableCopyOnRead20
Read_39/ReadVariableOpRead_39/ReadVariableOp24
Read_4/DisableCopyOnReadRead_4/DisableCopyOnRead2.
Read_4/ReadVariableOpRead_4/ReadVariableOp26
Read_40/DisableCopyOnReadRead_40/DisableCopyOnRead20
Read_40/ReadVariableOpRead_40/ReadVariableOp26
Read_41/DisableCopyOnReadRead_41/DisableCopyOnRead20
Read_41/ReadVariableOpRead_41/ReadVariableOp26
Read_42/DisableCopyOnReadRead_42/DisableCopyOnRead20
Read_42/ReadVariableOpRead_42/ReadVariableOp26
Read_43/DisableCopyOnReadRead_43/DisableCopyOnRead20
Read_43/ReadVariableOpRead_43/ReadVariableOp26
Read_44/DisableCopyOnReadRead_44/DisableCopyOnRead20
Read_44/ReadVariableOpRead_44/ReadVariableOp26
Read_45/DisableCopyOnReadRead_45/DisableCopyOnRead20
Read_45/ReadVariableOpRead_45/ReadVariableOp26
Read_46/DisableCopyOnReadRead_46/DisableCopyOnRead20
Read_46/ReadVariableOpRead_46/ReadVariableOp26
Read_47/DisableCopyOnReadRead_47/DisableCopyOnRead20
Read_47/ReadVariableOpRead_47/ReadVariableOp26
Read_48/DisableCopyOnReadRead_48/DisableCopyOnRead20
Read_48/ReadVariableOpRead_48/ReadVariableOp26
Read_49/DisableCopyOnReadRead_49/DisableCopyOnRead20
Read_49/ReadVariableOpRead_49/ReadVariableOp24
Read_5/DisableCopyOnReadRead_5/DisableCopyOnRead2.
Read_5/ReadVariableOpRead_5/ReadVariableOp26
Read_50/DisableCopyOnReadRead_50/DisableCopyOnRead20
Read_50/ReadVariableOpRead_50/ReadVariableOp26
Read_51/DisableCopyOnReadRead_51/DisableCopyOnRead20
Read_51/ReadVariableOpRead_51/ReadVariableOp26
Read_52/DisableCopyOnReadRead_52/DisableCopyOnRead20
Read_52/ReadVariableOpRead_52/ReadVariableOp26
Read_53/DisableCopyOnReadRead_53/DisableCopyOnRead20
Read_53/ReadVariableOpRead_53/ReadVariableOp26
Read_54/DisableCopyOnReadRead_54/DisableCopyOnRead20
Read_54/ReadVariableOpRead_54/ReadVariableOp26
Read_55/DisableCopyOnReadRead_55/DisableCopyOnRead20
Read_55/ReadVariableOpRead_55/ReadVariableOp26
Read_56/DisableCopyOnReadRead_56/DisableCopyOnRead20
Read_56/ReadVariableOpRead_56/ReadVariableOp26
Read_57/DisableCopyOnReadRead_57/DisableCopyOnRead20
Read_57/ReadVariableOpRead_57/ReadVariableOp26
Read_58/DisableCopyOnReadRead_58/DisableCopyOnRead20
Read_58/ReadVariableOpRead_58/ReadVariableOp26
Read_59/DisableCopyOnReadRead_59/DisableCopyOnRead20
Read_59/ReadVariableOpRead_59/ReadVariableOp24
Read_6/DisableCopyOnReadRead_6/DisableCopyOnRead2.
Read_6/ReadVariableOpRead_6/ReadVariableOp26
Read_60/DisableCopyOnReadRead_60/DisableCopyOnRead20
Read_60/ReadVariableOpRead_60/ReadVariableOp26
Read_61/DisableCopyOnReadRead_61/DisableCopyOnRead20
Read_61/ReadVariableOpRead_61/ReadVariableOp24
Read_7/DisableCopyOnReadRead_7/DisableCopyOnRead2.
Read_7/ReadVariableOpRead_7/ReadVariableOp24
Read_8/DisableCopyOnReadRead_8/DisableCopyOnRead2.
Read_8/ReadVariableOpRead_8/ReadVariableOp24
Read_9/DisableCopyOnReadRead_9/DisableCopyOnRead2.
Read_9/ReadVariableOpRead_9/ReadVariableOp:=?9

_output_shapes
: 

_user_specified_nameConst:/>+
)
_user_specified_namefalse_negatives:/=+
)
_user_specified_namefalse_positives:.<*
(
_user_specified_nametrue_negatives:.;*
(
_user_specified_nametrue_positives:1:-
+
_user_specified_namefalse_negatives_1:19-
+
_user_specified_namefalse_positives_1:08,
*
_user_specified_nametrue_negatives_1:07,
*
_user_specified_nametrue_positives_1:%6!

_user_specified_namecount:%5!

_user_specified_nametotal:'4#
!
_user_specified_name	count_1:'3#
!
_user_specified_name	total_1:32/
-
_user_specified_nameAdam/v/dense_6/bias:31/
-
_user_specified_nameAdam/m/dense_6/bias:501
/
_user_specified_nameAdam/v/dense_6/kernel:5/1
/
_user_specified_nameAdam/m/dense_6/kernel:3./
-
_user_specified_nameAdam/v/dense_5/bias:3-/
-
_user_specified_nameAdam/m/dense_5/bias:5,1
/
_user_specified_nameAdam/v/dense_5/kernel:5+1
/
_user_specified_nameAdam/m/dense_5/kernel:3*/
-
_user_specified_nameAdam/v/dense_4/bias:3)/
-
_user_specified_nameAdam/m/dense_4/bias:5(1
/
_user_specified_nameAdam/v/dense_4/kernel:5'1
/
_user_specified_nameAdam/m/dense_4/kernel:3&/
-
_user_specified_nameAdam/v/dense_3/bias:3%/
-
_user_specified_nameAdam/m/dense_3/bias:5$1
/
_user_specified_nameAdam/v/dense_3/kernel:5#1
/
_user_specified_nameAdam/m/dense_3/kernel:3"/
-
_user_specified_nameAdam/v/dense_2/bias:3!/
-
_user_specified_nameAdam/m/dense_2/bias:5 1
/
_user_specified_nameAdam/v/dense_2/kernel:51
/
_user_specified_nameAdam/m/dense_2/kernel:3/
-
_user_specified_nameAdam/v/dense_1/bias:3/
-
_user_specified_nameAdam/m/dense_1/bias:51
/
_user_specified_nameAdam/v/dense_1/kernel:51
/
_user_specified_nameAdam/m/dense_1/kernel:1-
+
_user_specified_nameAdam/v/dense/bias:1-
+
_user_specified_nameAdam/m/dense/bias:3/
-
_user_specified_nameAdam/v/dense/kernel:3/
-
_user_specified_nameAdam/m/dense/kernel:ZV
T
_user_specified_name<:Adam/v/dense_features_1/userId_embedding/embedding_weights:ZV
T
_user_specified_name<:Adam/m/dense_features_1/userId_embedding/embedding_weights:YU
S
_user_specified_name;9Adam/v/dense_features/movieId_embedding/embedding_weights:YU
S
_user_specified_name;9Adam/m/dense_features/movieId_embedding/embedding_weights:-)
'
_user_specified_namelearning_rate:)%
#
_user_specified_name	iteration:,(
&
_user_specified_namedense_6/bias:.*
(
_user_specified_namedense_6/kernel:,(
&
_user_specified_namedense_5/bias:.*
(
_user_specified_namedense_5/kernel:,(
&
_user_specified_namedense_4/bias:.*
(
_user_specified_namedense_4/kernel:,
(
&
_user_specified_namedense_3/bias:.	*
(
_user_specified_namedense_3/kernel:,(
&
_user_specified_namedense_2/bias:.*
(
_user_specified_namedense_2/kernel:,(
&
_user_specified_namedense_1/bias:.*
(
_user_specified_namedense_1/kernel:*&
$
_user_specified_name
dense/bias:,(
&
_user_specified_namedense/kernel:SO
M
_user_specified_name53dense_features_1/userId_embedding/embedding_weights:RN
L
_user_specified_name42dense_features/movieId_embedding/embedding_weights:C ?

_output_shapes
: 
%
_user_specified_namefile_prefix
�
�
(__inference_dense_4_layer_call_fn_403730

inputs
unknown:	@�
	unknown_0:	�
identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallinputsunknown	unknown_0*
Tin
2*
Tout
2*
_collective_manager_ids
 *(
_output_shapes
:����������*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_4_layer_call_and_return_conditional_losses_402788p
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*(
_output_shapes
:����������<
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������@: : 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403726:&"
 
_user_specified_name403724:O K
'
_output_shapes
:���������@
 
_user_specified_nameinputs
�
�
1__inference_dense_features_1_layer_call_fn_403473
features_movieid
features_userid
unknown:
��

identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallfeatures_movieidfeatures_useridunknown*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������
*#
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *U
fPRN
L__inference_dense_features_1_layer_call_and_return_conditional_losses_402927o
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:���������
<
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*3
_input_shapes"
 :���������:���������: 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403469:TP
#
_output_shapes
:���������
)
_user_specified_namefeatures_userid:U Q
#
_output_shapes
:���������
*
_user_specified_namefeatures_movieid
�y
�
L__inference_dense_features_1_layer_call_and_return_conditional_losses_402927
features

features_1m
Yuserid_embedding_userid_embedding_weights_embedding_lookup_sparse_embedding_lookup_402888:
��

identity��RuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupj
userId_embedding/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
����������
userId_embedding/ExpandDims
ExpandDims
features_1(userId_embedding/ExpandDims/dim:output:0*
T0*'
_output_shapes
:���������z
/userId_embedding/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB :
����������
)userId_embedding/to_sparse_input/NotEqualNotEqual$userId_embedding/ExpandDims:output:08userId_embedding/to_sparse_input/ignore_value/x:output:0*
T0*'
_output_shapes
:����������
(userId_embedding/to_sparse_input/indicesWhere-userId_embedding/to_sparse_input/NotEqual:z:0*'
_output_shapes
:����������
'userId_embedding/to_sparse_input/valuesGatherNd$userId_embedding/ExpandDims:output:00userId_embedding/to_sparse_input/indices:index:0*
Tindices0	*
Tparams0*#
_output_shapes
:����������
,userId_embedding/to_sparse_input/dense_shapeShape$userId_embedding/ExpandDims:output:0*
T0*
_output_shapes
:*
out_type0	:���
userId_embedding/valuesCast0userId_embedding/to_sparse_input/values:output:0*

DstT0	*

SrcT0*#
_output_shapes
:���������
5userId_embedding/userId_embedding_weights/Slice/beginConst*
_output_shapes
:*
dtype0*
valueB: ~
4userId_embedding/userId_embedding_weights/Slice/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
/userId_embedding/userId_embedding_weights/SliceSlice5userId_embedding/to_sparse_input/dense_shape:output:0>userId_embedding/userId_embedding_weights/Slice/begin:output:0=userId_embedding/userId_embedding_weights/Slice/size:output:0*
Index0*
T0	*
_output_shapes
:y
/userId_embedding/userId_embedding_weights/ConstConst*
_output_shapes
:*
dtype0*
valueB: �
.userId_embedding/userId_embedding_weights/ProdProd8userId_embedding/userId_embedding_weights/Slice:output:08userId_embedding/userId_embedding_weights/Const:output:0*
T0	*
_output_shapes
: |
:userId_embedding/userId_embedding_weights/GatherV2/indicesConst*
_output_shapes
: *
dtype0*
value	B :y
7userId_embedding/userId_embedding_weights/GatherV2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
2userId_embedding/userId_embedding_weights/GatherV2GatherV25userId_embedding/to_sparse_input/dense_shape:output:0CuserId_embedding/userId_embedding_weights/GatherV2/indices:output:0@userId_embedding/userId_embedding_weights/GatherV2/axis:output:0*
Taxis0*
Tindices0*
Tparams0	*
_output_shapes
: �
0userId_embedding/userId_embedding_weights/Cast/xPack7userId_embedding/userId_embedding_weights/Prod:output:0;userId_embedding/userId_embedding_weights/GatherV2:output:0*
N*
T0	*
_output_shapes
:�
7userId_embedding/userId_embedding_weights/SparseReshapeSparseReshape0userId_embedding/to_sparse_input/indices:index:05userId_embedding/to_sparse_input/dense_shape:output:09userId_embedding/userId_embedding_weights/Cast/x:output:0*-
_output_shapes
:���������:�
@userId_embedding/userId_embedding_weights/SparseReshape/IdentityIdentityuserId_embedding/values:y:0*
T0	*#
_output_shapes
:���������z
8userId_embedding/userId_embedding_weights/GreaterEqual/yConst*
_output_shapes
: *
dtype0	*
value	B	 R �
6userId_embedding/userId_embedding_weights/GreaterEqualGreaterEqualIuserId_embedding/userId_embedding_weights/SparseReshape/Identity:output:0AuserId_embedding/userId_embedding_weights/GreaterEqual/y:output:0*
T0	*#
_output_shapes
:����������
/userId_embedding/userId_embedding_weights/WhereWhere:userId_embedding/userId_embedding_weights/GreaterEqual:z:0*'
_output_shapes
:����������
7userId_embedding/userId_embedding_weights/Reshape/shapeConst*
_output_shapes
:*
dtype0*
valueB:
����������
1userId_embedding/userId_embedding_weights/ReshapeReshape7userId_embedding/userId_embedding_weights/Where:index:0@userId_embedding/userId_embedding_weights/Reshape/shape:output:0*
T0	*#
_output_shapes
:���������{
9userId_embedding/userId_embedding_weights/GatherV2_1/axisConst*
_output_shapes
: *
dtype0*
value	B : �
4userId_embedding/userId_embedding_weights/GatherV2_1GatherV2HuserId_embedding/userId_embedding_weights/SparseReshape:output_indices:0:userId_embedding/userId_embedding_weights/Reshape:output:0BuserId_embedding/userId_embedding_weights/GatherV2_1/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*'
_output_shapes
:���������{
9userId_embedding/userId_embedding_weights/GatherV2_2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
4userId_embedding/userId_embedding_weights/GatherV2_2GatherV2IuserId_embedding/userId_embedding_weights/SparseReshape/Identity:output:0:userId_embedding/userId_embedding_weights/Reshape:output:0BuserId_embedding/userId_embedding_weights/GatherV2_2/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*#
_output_shapes
:����������
2userId_embedding/userId_embedding_weights/IdentityIdentityFuserId_embedding/userId_embedding_weights/SparseReshape:output_shape:0*
T0	*
_output_shapes
:�
CuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/ConstConst*
_output_shapes
: *
dtype0	*
value	B	 R �
QuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRowsSparseFillEmptyRows=userId_embedding/userId_embedding_weights/GatherV2_1:output:0=userId_embedding/userId_embedding_weights/GatherV2_2:output:0;userId_embedding/userId_embedding_weights/Identity:output:0LuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/Const:output:0*
T0	*T
_output_shapesB
@:���������:���������:���������:����������
UuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB"        �
WuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB"       �
WuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB"      �
OuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_sliceStridedSlicebuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_indices:0^userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack:output:0`userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1:output:0`userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2:output:0*
Index0*
T0	*#
_output_shapes
:���������*

begin_mask*
end_mask*
shrink_axis_mask�
HuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/UniqueUniqueauserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_values:0*
T0	*2
_output_shapes 
:���������:����������
RuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupResourceGatherYuserid_embedding_userid_embedding_weights_embedding_lookup_sparse_embedding_lookup_402888LuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/Unique:y:0*
Tindices0	*l
_classb
`^loc:@userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/402888*'
_output_shapes
:���������
*
dtype0�
[userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/IdentityIdentity[userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup:output:0*
T0*'
_output_shapes
:���������
�
AuserId_embedding/userId_embedding_weights/embedding_lookup_sparseSparseSegmentMeanduserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/Identity:output:0NuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/Unique:idx:0XuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice:output:0*
Tsegmentids0	*
T0*'
_output_shapes
:���������
�
9userId_embedding/userId_embedding_weights/Reshape_1/shapeConst*
_output_shapes
:*
dtype0*
valueB"����   �
3userId_embedding/userId_embedding_weights/Reshape_1ReshapeguserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:empty_row_indicator:0BuserId_embedding/userId_embedding_weights/Reshape_1/shape:output:0*
T0
*'
_output_shapes
:����������
/userId_embedding/userId_embedding_weights/ShapeShapeJuserId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*
_output_shapes
::���
=userId_embedding/userId_embedding_weights/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB:�
?userId_embedding/userId_embedding_weights/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:�
?userId_embedding/userId_embedding_weights/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
7userId_embedding/userId_embedding_weights/strided_sliceStridedSlice8userId_embedding/userId_embedding_weights/Shape:output:0FuserId_embedding/userId_embedding_weights/strided_slice/stack:output:0HuserId_embedding/userId_embedding_weights/strided_slice/stack_1:output:0HuserId_embedding/userId_embedding_weights/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_masks
1userId_embedding/userId_embedding_weights/stack/0Const*
_output_shapes
: *
dtype0*
value	B :�
/userId_embedding/userId_embedding_weights/stackPack:userId_embedding/userId_embedding_weights/stack/0:output:0@userId_embedding/userId_embedding_weights/strided_slice:output:0*
N*
T0*
_output_shapes
:�
.userId_embedding/userId_embedding_weights/TileTile<userId_embedding/userId_embedding_weights/Reshape_1:output:08userId_embedding/userId_embedding_weights/stack:output:0*
T0
*'
_output_shapes
:���������
�
4userId_embedding/userId_embedding_weights/zeros_like	ZerosLikeJuserId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
)userId_embedding/userId_embedding_weightsSelect7userId_embedding/userId_embedding_weights/Tile:output:08userId_embedding/userId_embedding_weights/zeros_like:y:0JuserId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
0userId_embedding/userId_embedding_weights/Cast_1Cast5userId_embedding/to_sparse_input/dense_shape:output:0*

DstT0*

SrcT0	*
_output_shapes
:�
7userId_embedding/userId_embedding_weights/Slice_1/beginConst*
_output_shapes
:*
dtype0*
valueB: �
6userId_embedding/userId_embedding_weights/Slice_1/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
1userId_embedding/userId_embedding_weights/Slice_1Slice4userId_embedding/userId_embedding_weights/Cast_1:y:0@userId_embedding/userId_embedding_weights/Slice_1/begin:output:0?userId_embedding/userId_embedding_weights/Slice_1/size:output:0*
Index0*
T0*
_output_shapes
:�
1userId_embedding/userId_embedding_weights/Shape_1Shape2userId_embedding/userId_embedding_weights:output:0*
T0*
_output_shapes
::���
7userId_embedding/userId_embedding_weights/Slice_2/beginConst*
_output_shapes
:*
dtype0*
valueB:�
6userId_embedding/userId_embedding_weights/Slice_2/sizeConst*
_output_shapes
:*
dtype0*
valueB:
����������
1userId_embedding/userId_embedding_weights/Slice_2Slice:userId_embedding/userId_embedding_weights/Shape_1:output:0@userId_embedding/userId_embedding_weights/Slice_2/begin:output:0?userId_embedding/userId_embedding_weights/Slice_2/size:output:0*
Index0*
T0*
_output_shapes
:w
5userId_embedding/userId_embedding_weights/concat/axisConst*
_output_shapes
: *
dtype0*
value	B : �
0userId_embedding/userId_embedding_weights/concatConcatV2:userId_embedding/userId_embedding_weights/Slice_1:output:0:userId_embedding/userId_embedding_weights/Slice_2:output:0>userId_embedding/userId_embedding_weights/concat/axis:output:0*
N*
T0*
_output_shapes
:�
3userId_embedding/userId_embedding_weights/Reshape_2Reshape2userId_embedding/userId_embedding_weights:output:09userId_embedding/userId_embedding_weights/concat:output:0*
T0*'
_output_shapes
:���������
�
userId_embedding/ShapeShape<userId_embedding/userId_embedding_weights/Reshape_2:output:0*
T0*
_output_shapes
::��n
$userId_embedding/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: p
&userId_embedding/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:p
&userId_embedding/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
userId_embedding/strided_sliceStridedSliceuserId_embedding/Shape:output:0-userId_embedding/strided_slice/stack:output:0/userId_embedding/strided_slice/stack_1:output:0/userId_embedding/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_maskb
 userId_embedding/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
�
userId_embedding/Reshape/shapePack'userId_embedding/strided_slice:output:0)userId_embedding/Reshape/shape/1:output:0*
N*
T0*
_output_shapes
:�
userId_embedding/ReshapeReshape<userId_embedding/userId_embedding_weights/Reshape_2:output:0'userId_embedding/Reshape/shape:output:0*
T0*'
_output_shapes
:���������
\
concat/concat_dimConst*
_output_shapes
: *
dtype0*
valueB :
���������n
concat/concatIdentity!userId_embedding/Reshape:output:0*
T0*'
_output_shapes
:���������
e
IdentityIdentityconcat/concat:output:0^NoOp*
T0*'
_output_shapes
:���������
w
NoOpNoOpS^userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*3
_input_shapes"
 :���������:���������: 2�
RuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupRuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup:&"
 
_user_specified_name402888:MI
#
_output_shapes
:���������
"
_user_specified_name
features:M I
#
_output_shapes
:���������
"
_user_specified_name
features
�z
�
L__inference_dense_features_1_layer_call_and_return_conditional_losses_403641
features_movieid
features_useridm
Yuserid_embedding_userid_embedding_weights_embedding_lookup_sparse_embedding_lookup_403602:
��

identity��RuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupj
userId_embedding/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
����������
userId_embedding/ExpandDims
ExpandDimsfeatures_userid(userId_embedding/ExpandDims/dim:output:0*
T0*'
_output_shapes
:���������z
/userId_embedding/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB :
����������
)userId_embedding/to_sparse_input/NotEqualNotEqual$userId_embedding/ExpandDims:output:08userId_embedding/to_sparse_input/ignore_value/x:output:0*
T0*'
_output_shapes
:����������
(userId_embedding/to_sparse_input/indicesWhere-userId_embedding/to_sparse_input/NotEqual:z:0*'
_output_shapes
:����������
'userId_embedding/to_sparse_input/valuesGatherNd$userId_embedding/ExpandDims:output:00userId_embedding/to_sparse_input/indices:index:0*
Tindices0	*
Tparams0*#
_output_shapes
:����������
,userId_embedding/to_sparse_input/dense_shapeShape$userId_embedding/ExpandDims:output:0*
T0*
_output_shapes
:*
out_type0	:���
userId_embedding/valuesCast0userId_embedding/to_sparse_input/values:output:0*

DstT0	*

SrcT0*#
_output_shapes
:���������
5userId_embedding/userId_embedding_weights/Slice/beginConst*
_output_shapes
:*
dtype0*
valueB: ~
4userId_embedding/userId_embedding_weights/Slice/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
/userId_embedding/userId_embedding_weights/SliceSlice5userId_embedding/to_sparse_input/dense_shape:output:0>userId_embedding/userId_embedding_weights/Slice/begin:output:0=userId_embedding/userId_embedding_weights/Slice/size:output:0*
Index0*
T0	*
_output_shapes
:y
/userId_embedding/userId_embedding_weights/ConstConst*
_output_shapes
:*
dtype0*
valueB: �
.userId_embedding/userId_embedding_weights/ProdProd8userId_embedding/userId_embedding_weights/Slice:output:08userId_embedding/userId_embedding_weights/Const:output:0*
T0	*
_output_shapes
: |
:userId_embedding/userId_embedding_weights/GatherV2/indicesConst*
_output_shapes
: *
dtype0*
value	B :y
7userId_embedding/userId_embedding_weights/GatherV2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
2userId_embedding/userId_embedding_weights/GatherV2GatherV25userId_embedding/to_sparse_input/dense_shape:output:0CuserId_embedding/userId_embedding_weights/GatherV2/indices:output:0@userId_embedding/userId_embedding_weights/GatherV2/axis:output:0*
Taxis0*
Tindices0*
Tparams0	*
_output_shapes
: �
0userId_embedding/userId_embedding_weights/Cast/xPack7userId_embedding/userId_embedding_weights/Prod:output:0;userId_embedding/userId_embedding_weights/GatherV2:output:0*
N*
T0	*
_output_shapes
:�
7userId_embedding/userId_embedding_weights/SparseReshapeSparseReshape0userId_embedding/to_sparse_input/indices:index:05userId_embedding/to_sparse_input/dense_shape:output:09userId_embedding/userId_embedding_weights/Cast/x:output:0*-
_output_shapes
:���������:�
@userId_embedding/userId_embedding_weights/SparseReshape/IdentityIdentityuserId_embedding/values:y:0*
T0	*#
_output_shapes
:���������z
8userId_embedding/userId_embedding_weights/GreaterEqual/yConst*
_output_shapes
: *
dtype0	*
value	B	 R �
6userId_embedding/userId_embedding_weights/GreaterEqualGreaterEqualIuserId_embedding/userId_embedding_weights/SparseReshape/Identity:output:0AuserId_embedding/userId_embedding_weights/GreaterEqual/y:output:0*
T0	*#
_output_shapes
:����������
/userId_embedding/userId_embedding_weights/WhereWhere:userId_embedding/userId_embedding_weights/GreaterEqual:z:0*'
_output_shapes
:����������
7userId_embedding/userId_embedding_weights/Reshape/shapeConst*
_output_shapes
:*
dtype0*
valueB:
����������
1userId_embedding/userId_embedding_weights/ReshapeReshape7userId_embedding/userId_embedding_weights/Where:index:0@userId_embedding/userId_embedding_weights/Reshape/shape:output:0*
T0	*#
_output_shapes
:���������{
9userId_embedding/userId_embedding_weights/GatherV2_1/axisConst*
_output_shapes
: *
dtype0*
value	B : �
4userId_embedding/userId_embedding_weights/GatherV2_1GatherV2HuserId_embedding/userId_embedding_weights/SparseReshape:output_indices:0:userId_embedding/userId_embedding_weights/Reshape:output:0BuserId_embedding/userId_embedding_weights/GatherV2_1/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*'
_output_shapes
:���������{
9userId_embedding/userId_embedding_weights/GatherV2_2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
4userId_embedding/userId_embedding_weights/GatherV2_2GatherV2IuserId_embedding/userId_embedding_weights/SparseReshape/Identity:output:0:userId_embedding/userId_embedding_weights/Reshape:output:0BuserId_embedding/userId_embedding_weights/GatherV2_2/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*#
_output_shapes
:����������
2userId_embedding/userId_embedding_weights/IdentityIdentityFuserId_embedding/userId_embedding_weights/SparseReshape:output_shape:0*
T0	*
_output_shapes
:�
CuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/ConstConst*
_output_shapes
: *
dtype0	*
value	B	 R �
QuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRowsSparseFillEmptyRows=userId_embedding/userId_embedding_weights/GatherV2_1:output:0=userId_embedding/userId_embedding_weights/GatherV2_2:output:0;userId_embedding/userId_embedding_weights/Identity:output:0LuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/Const:output:0*
T0	*T
_output_shapesB
@:���������:���������:���������:����������
UuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB"        �
WuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB"       �
WuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB"      �
OuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_sliceStridedSlicebuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_indices:0^userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack:output:0`userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1:output:0`userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2:output:0*
Index0*
T0	*#
_output_shapes
:���������*

begin_mask*
end_mask*
shrink_axis_mask�
HuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/UniqueUniqueauserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_values:0*
T0	*2
_output_shapes 
:���������:����������
RuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupResourceGatherYuserid_embedding_userid_embedding_weights_embedding_lookup_sparse_embedding_lookup_403602LuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/Unique:y:0*
Tindices0	*l
_classb
`^loc:@userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/403602*'
_output_shapes
:���������
*
dtype0�
[userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/IdentityIdentity[userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup:output:0*
T0*'
_output_shapes
:���������
�
AuserId_embedding/userId_embedding_weights/embedding_lookup_sparseSparseSegmentMeanduserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/Identity:output:0NuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/Unique:idx:0XuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice:output:0*
Tsegmentids0	*
T0*'
_output_shapes
:���������
�
9userId_embedding/userId_embedding_weights/Reshape_1/shapeConst*
_output_shapes
:*
dtype0*
valueB"����   �
3userId_embedding/userId_embedding_weights/Reshape_1ReshapeguserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:empty_row_indicator:0BuserId_embedding/userId_embedding_weights/Reshape_1/shape:output:0*
T0
*'
_output_shapes
:����������
/userId_embedding/userId_embedding_weights/ShapeShapeJuserId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*
_output_shapes
::���
=userId_embedding/userId_embedding_weights/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB:�
?userId_embedding/userId_embedding_weights/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:�
?userId_embedding/userId_embedding_weights/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
7userId_embedding/userId_embedding_weights/strided_sliceStridedSlice8userId_embedding/userId_embedding_weights/Shape:output:0FuserId_embedding/userId_embedding_weights/strided_slice/stack:output:0HuserId_embedding/userId_embedding_weights/strided_slice/stack_1:output:0HuserId_embedding/userId_embedding_weights/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_masks
1userId_embedding/userId_embedding_weights/stack/0Const*
_output_shapes
: *
dtype0*
value	B :�
/userId_embedding/userId_embedding_weights/stackPack:userId_embedding/userId_embedding_weights/stack/0:output:0@userId_embedding/userId_embedding_weights/strided_slice:output:0*
N*
T0*
_output_shapes
:�
.userId_embedding/userId_embedding_weights/TileTile<userId_embedding/userId_embedding_weights/Reshape_1:output:08userId_embedding/userId_embedding_weights/stack:output:0*
T0
*'
_output_shapes
:���������
�
4userId_embedding/userId_embedding_weights/zeros_like	ZerosLikeJuserId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
)userId_embedding/userId_embedding_weightsSelect7userId_embedding/userId_embedding_weights/Tile:output:08userId_embedding/userId_embedding_weights/zeros_like:y:0JuserId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
0userId_embedding/userId_embedding_weights/Cast_1Cast5userId_embedding/to_sparse_input/dense_shape:output:0*

DstT0*

SrcT0	*
_output_shapes
:�
7userId_embedding/userId_embedding_weights/Slice_1/beginConst*
_output_shapes
:*
dtype0*
valueB: �
6userId_embedding/userId_embedding_weights/Slice_1/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
1userId_embedding/userId_embedding_weights/Slice_1Slice4userId_embedding/userId_embedding_weights/Cast_1:y:0@userId_embedding/userId_embedding_weights/Slice_1/begin:output:0?userId_embedding/userId_embedding_weights/Slice_1/size:output:0*
Index0*
T0*
_output_shapes
:�
1userId_embedding/userId_embedding_weights/Shape_1Shape2userId_embedding/userId_embedding_weights:output:0*
T0*
_output_shapes
::���
7userId_embedding/userId_embedding_weights/Slice_2/beginConst*
_output_shapes
:*
dtype0*
valueB:�
6userId_embedding/userId_embedding_weights/Slice_2/sizeConst*
_output_shapes
:*
dtype0*
valueB:
����������
1userId_embedding/userId_embedding_weights/Slice_2Slice:userId_embedding/userId_embedding_weights/Shape_1:output:0@userId_embedding/userId_embedding_weights/Slice_2/begin:output:0?userId_embedding/userId_embedding_weights/Slice_2/size:output:0*
Index0*
T0*
_output_shapes
:w
5userId_embedding/userId_embedding_weights/concat/axisConst*
_output_shapes
: *
dtype0*
value	B : �
0userId_embedding/userId_embedding_weights/concatConcatV2:userId_embedding/userId_embedding_weights/Slice_1:output:0:userId_embedding/userId_embedding_weights/Slice_2:output:0>userId_embedding/userId_embedding_weights/concat/axis:output:0*
N*
T0*
_output_shapes
:�
3userId_embedding/userId_embedding_weights/Reshape_2Reshape2userId_embedding/userId_embedding_weights:output:09userId_embedding/userId_embedding_weights/concat:output:0*
T0*'
_output_shapes
:���������
�
userId_embedding/ShapeShape<userId_embedding/userId_embedding_weights/Reshape_2:output:0*
T0*
_output_shapes
::��n
$userId_embedding/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: p
&userId_embedding/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:p
&userId_embedding/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
userId_embedding/strided_sliceStridedSliceuserId_embedding/Shape:output:0-userId_embedding/strided_slice/stack:output:0/userId_embedding/strided_slice/stack_1:output:0/userId_embedding/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_maskb
 userId_embedding/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
�
userId_embedding/Reshape/shapePack'userId_embedding/strided_slice:output:0)userId_embedding/Reshape/shape/1:output:0*
N*
T0*
_output_shapes
:�
userId_embedding/ReshapeReshape<userId_embedding/userId_embedding_weights/Reshape_2:output:0'userId_embedding/Reshape/shape:output:0*
T0*'
_output_shapes
:���������
\
concat/concat_dimConst*
_output_shapes
: *
dtype0*
valueB :
���������n
concat/concatIdentity!userId_embedding/Reshape:output:0*
T0*'
_output_shapes
:���������
e
IdentityIdentityconcat/concat:output:0^NoOp*
T0*'
_output_shapes
:���������
w
NoOpNoOpS^userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*3
_input_shapes"
 :���������:���������: 2�
RuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupRuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup:&"
 
_user_specified_name403602:TP
#
_output_shapes
:���������
)
_user_specified_namefeatures_userid:U Q
#
_output_shapes
:���������
*
_user_specified_namefeatures_movieid
�

�
A__inference_dense_layer_call_and_return_conditional_losses_402740

inputs0
matmul_readvariableop_resource:
 -
biasadd_readvariableop_resource: 
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpt
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes

:
 *
dtype0i
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:��������� r
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes
: *
dtype0v
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:��������� P
ReluReluBiasAdd:output:0*
T0*'
_output_shapes
:��������� a
IdentityIdentityRelu:activations:0^NoOp*
T0*'
_output_shapes
:��������� S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������
: : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:���������

 
_user_specified_nameinputs
�
P
$__inference_dot_layer_call_fn_403767
inputs_0
inputs_1
identity�
PartitionedCallPartitionedCallinputs_0inputs_1*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������* 
_read_only_resource_inputs
 *-
config_proto

CPU

GPU 2J 8� *H
fCRA
?__inference_dot_layer_call_and_return_conditional_losses_402821`
IdentityIdentityPartitionedCall:output:0*
T0*'
_output_shapes
:���������"
identityIdentity:output:0*(
_construction_contextkEagerRuntime*;
_input_shapes*
(:����������:����������:RN
(
_output_shapes
:����������
"
_user_specified_name
inputs_1:R N
(
_output_shapes
:����������
"
_user_specified_name
inputs_0
�z
�
L__inference_dense_features_1_layer_call_and_return_conditional_losses_403557
features_movieid
features_useridm
Yuserid_embedding_userid_embedding_weights_embedding_lookup_sparse_embedding_lookup_403518:
��

identity��RuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupj
userId_embedding/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
����������
userId_embedding/ExpandDims
ExpandDimsfeatures_userid(userId_embedding/ExpandDims/dim:output:0*
T0*'
_output_shapes
:���������z
/userId_embedding/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB :
����������
)userId_embedding/to_sparse_input/NotEqualNotEqual$userId_embedding/ExpandDims:output:08userId_embedding/to_sparse_input/ignore_value/x:output:0*
T0*'
_output_shapes
:����������
(userId_embedding/to_sparse_input/indicesWhere-userId_embedding/to_sparse_input/NotEqual:z:0*'
_output_shapes
:����������
'userId_embedding/to_sparse_input/valuesGatherNd$userId_embedding/ExpandDims:output:00userId_embedding/to_sparse_input/indices:index:0*
Tindices0	*
Tparams0*#
_output_shapes
:����������
,userId_embedding/to_sparse_input/dense_shapeShape$userId_embedding/ExpandDims:output:0*
T0*
_output_shapes
:*
out_type0	:���
userId_embedding/valuesCast0userId_embedding/to_sparse_input/values:output:0*

DstT0	*

SrcT0*#
_output_shapes
:���������
5userId_embedding/userId_embedding_weights/Slice/beginConst*
_output_shapes
:*
dtype0*
valueB: ~
4userId_embedding/userId_embedding_weights/Slice/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
/userId_embedding/userId_embedding_weights/SliceSlice5userId_embedding/to_sparse_input/dense_shape:output:0>userId_embedding/userId_embedding_weights/Slice/begin:output:0=userId_embedding/userId_embedding_weights/Slice/size:output:0*
Index0*
T0	*
_output_shapes
:y
/userId_embedding/userId_embedding_weights/ConstConst*
_output_shapes
:*
dtype0*
valueB: �
.userId_embedding/userId_embedding_weights/ProdProd8userId_embedding/userId_embedding_weights/Slice:output:08userId_embedding/userId_embedding_weights/Const:output:0*
T0	*
_output_shapes
: |
:userId_embedding/userId_embedding_weights/GatherV2/indicesConst*
_output_shapes
: *
dtype0*
value	B :y
7userId_embedding/userId_embedding_weights/GatherV2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
2userId_embedding/userId_embedding_weights/GatherV2GatherV25userId_embedding/to_sparse_input/dense_shape:output:0CuserId_embedding/userId_embedding_weights/GatherV2/indices:output:0@userId_embedding/userId_embedding_weights/GatherV2/axis:output:0*
Taxis0*
Tindices0*
Tparams0	*
_output_shapes
: �
0userId_embedding/userId_embedding_weights/Cast/xPack7userId_embedding/userId_embedding_weights/Prod:output:0;userId_embedding/userId_embedding_weights/GatherV2:output:0*
N*
T0	*
_output_shapes
:�
7userId_embedding/userId_embedding_weights/SparseReshapeSparseReshape0userId_embedding/to_sparse_input/indices:index:05userId_embedding/to_sparse_input/dense_shape:output:09userId_embedding/userId_embedding_weights/Cast/x:output:0*-
_output_shapes
:���������:�
@userId_embedding/userId_embedding_weights/SparseReshape/IdentityIdentityuserId_embedding/values:y:0*
T0	*#
_output_shapes
:���������z
8userId_embedding/userId_embedding_weights/GreaterEqual/yConst*
_output_shapes
: *
dtype0	*
value	B	 R �
6userId_embedding/userId_embedding_weights/GreaterEqualGreaterEqualIuserId_embedding/userId_embedding_weights/SparseReshape/Identity:output:0AuserId_embedding/userId_embedding_weights/GreaterEqual/y:output:0*
T0	*#
_output_shapes
:����������
/userId_embedding/userId_embedding_weights/WhereWhere:userId_embedding/userId_embedding_weights/GreaterEqual:z:0*'
_output_shapes
:����������
7userId_embedding/userId_embedding_weights/Reshape/shapeConst*
_output_shapes
:*
dtype0*
valueB:
����������
1userId_embedding/userId_embedding_weights/ReshapeReshape7userId_embedding/userId_embedding_weights/Where:index:0@userId_embedding/userId_embedding_weights/Reshape/shape:output:0*
T0	*#
_output_shapes
:���������{
9userId_embedding/userId_embedding_weights/GatherV2_1/axisConst*
_output_shapes
: *
dtype0*
value	B : �
4userId_embedding/userId_embedding_weights/GatherV2_1GatherV2HuserId_embedding/userId_embedding_weights/SparseReshape:output_indices:0:userId_embedding/userId_embedding_weights/Reshape:output:0BuserId_embedding/userId_embedding_weights/GatherV2_1/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*'
_output_shapes
:���������{
9userId_embedding/userId_embedding_weights/GatherV2_2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
4userId_embedding/userId_embedding_weights/GatherV2_2GatherV2IuserId_embedding/userId_embedding_weights/SparseReshape/Identity:output:0:userId_embedding/userId_embedding_weights/Reshape:output:0BuserId_embedding/userId_embedding_weights/GatherV2_2/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*#
_output_shapes
:����������
2userId_embedding/userId_embedding_weights/IdentityIdentityFuserId_embedding/userId_embedding_weights/SparseReshape:output_shape:0*
T0	*
_output_shapes
:�
CuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/ConstConst*
_output_shapes
: *
dtype0	*
value	B	 R �
QuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRowsSparseFillEmptyRows=userId_embedding/userId_embedding_weights/GatherV2_1:output:0=userId_embedding/userId_embedding_weights/GatherV2_2:output:0;userId_embedding/userId_embedding_weights/Identity:output:0LuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/Const:output:0*
T0	*T
_output_shapesB
@:���������:���������:���������:����������
UuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB"        �
WuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB"       �
WuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB"      �
OuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_sliceStridedSlicebuserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_indices:0^userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack:output:0`userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1:output:0`userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2:output:0*
Index0*
T0	*#
_output_shapes
:���������*

begin_mask*
end_mask*
shrink_axis_mask�
HuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/UniqueUniqueauserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_values:0*
T0	*2
_output_shapes 
:���������:����������
RuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupResourceGatherYuserid_embedding_userid_embedding_weights_embedding_lookup_sparse_embedding_lookup_403518LuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/Unique:y:0*
Tindices0	*l
_classb
`^loc:@userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/403518*'
_output_shapes
:���������
*
dtype0�
[userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/IdentityIdentity[userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup:output:0*
T0*'
_output_shapes
:���������
�
AuserId_embedding/userId_embedding_weights/embedding_lookup_sparseSparseSegmentMeanduserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/Identity:output:0NuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/Unique:idx:0XuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice:output:0*
Tsegmentids0	*
T0*'
_output_shapes
:���������
�
9userId_embedding/userId_embedding_weights/Reshape_1/shapeConst*
_output_shapes
:*
dtype0*
valueB"����   �
3userId_embedding/userId_embedding_weights/Reshape_1ReshapeguserId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:empty_row_indicator:0BuserId_embedding/userId_embedding_weights/Reshape_1/shape:output:0*
T0
*'
_output_shapes
:����������
/userId_embedding/userId_embedding_weights/ShapeShapeJuserId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*
_output_shapes
::���
=userId_embedding/userId_embedding_weights/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB:�
?userId_embedding/userId_embedding_weights/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:�
?userId_embedding/userId_embedding_weights/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
7userId_embedding/userId_embedding_weights/strided_sliceStridedSlice8userId_embedding/userId_embedding_weights/Shape:output:0FuserId_embedding/userId_embedding_weights/strided_slice/stack:output:0HuserId_embedding/userId_embedding_weights/strided_slice/stack_1:output:0HuserId_embedding/userId_embedding_weights/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_masks
1userId_embedding/userId_embedding_weights/stack/0Const*
_output_shapes
: *
dtype0*
value	B :�
/userId_embedding/userId_embedding_weights/stackPack:userId_embedding/userId_embedding_weights/stack/0:output:0@userId_embedding/userId_embedding_weights/strided_slice:output:0*
N*
T0*
_output_shapes
:�
.userId_embedding/userId_embedding_weights/TileTile<userId_embedding/userId_embedding_weights/Reshape_1:output:08userId_embedding/userId_embedding_weights/stack:output:0*
T0
*'
_output_shapes
:���������
�
4userId_embedding/userId_embedding_weights/zeros_like	ZerosLikeJuserId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
)userId_embedding/userId_embedding_weightsSelect7userId_embedding/userId_embedding_weights/Tile:output:08userId_embedding/userId_embedding_weights/zeros_like:y:0JuserId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
0userId_embedding/userId_embedding_weights/Cast_1Cast5userId_embedding/to_sparse_input/dense_shape:output:0*

DstT0*

SrcT0	*
_output_shapes
:�
7userId_embedding/userId_embedding_weights/Slice_1/beginConst*
_output_shapes
:*
dtype0*
valueB: �
6userId_embedding/userId_embedding_weights/Slice_1/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
1userId_embedding/userId_embedding_weights/Slice_1Slice4userId_embedding/userId_embedding_weights/Cast_1:y:0@userId_embedding/userId_embedding_weights/Slice_1/begin:output:0?userId_embedding/userId_embedding_weights/Slice_1/size:output:0*
Index0*
T0*
_output_shapes
:�
1userId_embedding/userId_embedding_weights/Shape_1Shape2userId_embedding/userId_embedding_weights:output:0*
T0*
_output_shapes
::���
7userId_embedding/userId_embedding_weights/Slice_2/beginConst*
_output_shapes
:*
dtype0*
valueB:�
6userId_embedding/userId_embedding_weights/Slice_2/sizeConst*
_output_shapes
:*
dtype0*
valueB:
����������
1userId_embedding/userId_embedding_weights/Slice_2Slice:userId_embedding/userId_embedding_weights/Shape_1:output:0@userId_embedding/userId_embedding_weights/Slice_2/begin:output:0?userId_embedding/userId_embedding_weights/Slice_2/size:output:0*
Index0*
T0*
_output_shapes
:w
5userId_embedding/userId_embedding_weights/concat/axisConst*
_output_shapes
: *
dtype0*
value	B : �
0userId_embedding/userId_embedding_weights/concatConcatV2:userId_embedding/userId_embedding_weights/Slice_1:output:0:userId_embedding/userId_embedding_weights/Slice_2:output:0>userId_embedding/userId_embedding_weights/concat/axis:output:0*
N*
T0*
_output_shapes
:�
3userId_embedding/userId_embedding_weights/Reshape_2Reshape2userId_embedding/userId_embedding_weights:output:09userId_embedding/userId_embedding_weights/concat:output:0*
T0*'
_output_shapes
:���������
�
userId_embedding/ShapeShape<userId_embedding/userId_embedding_weights/Reshape_2:output:0*
T0*
_output_shapes
::��n
$userId_embedding/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: p
&userId_embedding/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:p
&userId_embedding/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
userId_embedding/strided_sliceStridedSliceuserId_embedding/Shape:output:0-userId_embedding/strided_slice/stack:output:0/userId_embedding/strided_slice/stack_1:output:0/userId_embedding/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_maskb
 userId_embedding/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
�
userId_embedding/Reshape/shapePack'userId_embedding/strided_slice:output:0)userId_embedding/Reshape/shape/1:output:0*
N*
T0*
_output_shapes
:�
userId_embedding/ReshapeReshape<userId_embedding/userId_embedding_weights/Reshape_2:output:0'userId_embedding/Reshape/shape:output:0*
T0*'
_output_shapes
:���������
\
concat/concat_dimConst*
_output_shapes
: *
dtype0*
valueB :
���������n
concat/concatIdentity!userId_embedding/Reshape:output:0*
T0*'
_output_shapes
:���������
e
IdentityIdentityconcat/concat:output:0^NoOp*
T0*'
_output_shapes
:���������
w
NoOpNoOpS^userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*3
_input_shapes"
 :���������:���������: 2�
RuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupRuserId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup:&"
 
_user_specified_name403518:TP
#
_output_shapes
:���������
)
_user_specified_namefeatures_userid:U Q
#
_output_shapes
:���������
*
_user_specified_namefeatures_movieid
�	
i
?__inference_dot_layer_call_and_return_conditional_losses_402821

inputs
inputs_1
identityP
ExpandDims/dimConst*
_output_shapes
: *
dtype0*
value	B :p

ExpandDims
ExpandDimsinputsExpandDims/dim:output:0*
T0*,
_output_shapes
:����������R
ExpandDims_1/dimConst*
_output_shapes
: *
dtype0*
value	B :v
ExpandDims_1
ExpandDimsinputs_1ExpandDims_1/dim:output:0*
T0*,
_output_shapes
:����������y
MatMulBatchMatMulV2ExpandDims:output:0ExpandDims_1:output:0*
T0*+
_output_shapes
:���������R
ShapeShapeMatMul:output:0*
T0*
_output_shapes
::��l
SqueezeSqueezeMatMul:output:0*
T0*'
_output_shapes
:���������*
squeeze_dims
X
IdentityIdentitySqueeze:output:0*
T0*'
_output_shapes
:���������"
identityIdentity:output:0*(
_construction_contextkEagerRuntime*;
_input_shapes*
(:����������:����������:PL
(
_output_shapes
:����������
 
_user_specified_nameinputs:P L
(
_output_shapes
:����������
 
_user_specified_nameinputs
�
�
(__inference_dense_2_layer_call_fn_403690

inputs
unknown: @
	unknown_0:@
identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallinputsunknown	unknown_0*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������@*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_2_layer_call_and_return_conditional_losses_402772o
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:���������@<
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:��������� : : 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403686:&"
 
_user_specified_name403684:O K
'
_output_shapes
:��������� 
 
_user_specified_nameinputs
�6
�
A__inference_model_layer_call_and_return_conditional_losses_403055
movieid

userid+
dense_features_1_402928:
��
)
dense_features_403015:
��
 
dense_1_403018:
 
dense_1_403020: 
dense_403023:
 
dense_403025:  
dense_3_403028: @
dense_3_403030:@ 
dense_2_403033: @
dense_2_403035:@!
dense_4_403038:	@�
dense_4_403040:	�!
dense_5_403043:	@�
dense_5_403045:	� 
dense_6_403049:
dense_6_403051:
identity��dense/StatefulPartitionedCall�dense_1/StatefulPartitionedCall�dense_2/StatefulPartitionedCall�dense_3/StatefulPartitionedCall�dense_4/StatefulPartitionedCall�dense_5/StatefulPartitionedCall�dense_6/StatefulPartitionedCall�&dense_features/StatefulPartitionedCall�(dense_features_1/StatefulPartitionedCall�
(dense_features_1/StatefulPartitionedCallStatefulPartitionedCallmovieiduseriddense_features_1_402928*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������
*#
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *U
fPRN
L__inference_dense_features_1_layer_call_and_return_conditional_losses_402927�
&dense_features/StatefulPartitionedCallStatefulPartitionedCallmovieiduseriddense_features_403015*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������
*#
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *S
fNRL
J__inference_dense_features_layer_call_and_return_conditional_losses_403014�
dense_1/StatefulPartitionedCallStatefulPartitionedCall1dense_features_1/StatefulPartitionedCall:output:0dense_1_403018dense_1_403020*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:��������� *$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_1_layer_call_and_return_conditional_losses_402724�
dense/StatefulPartitionedCallStatefulPartitionedCall/dense_features/StatefulPartitionedCall:output:0dense_403023dense_403025*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:��������� *$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *J
fERC
A__inference_dense_layer_call_and_return_conditional_losses_402740�
dense_3/StatefulPartitionedCallStatefulPartitionedCall(dense_1/StatefulPartitionedCall:output:0dense_3_403028dense_3_403030*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������@*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_3_layer_call_and_return_conditional_losses_402756�
dense_2/StatefulPartitionedCallStatefulPartitionedCall&dense/StatefulPartitionedCall:output:0dense_2_403033dense_2_403035*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������@*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_2_layer_call_and_return_conditional_losses_402772�
dense_4/StatefulPartitionedCallStatefulPartitionedCall(dense_2/StatefulPartitionedCall:output:0dense_4_403038dense_4_403040*
Tin
2*
Tout
2*
_collective_manager_ids
 *(
_output_shapes
:����������*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_4_layer_call_and_return_conditional_losses_402788�
dense_5/StatefulPartitionedCallStatefulPartitionedCall(dense_3/StatefulPartitionedCall:output:0dense_5_403043dense_5_403045*
Tin
2*
Tout
2*
_collective_manager_ids
 *(
_output_shapes
:����������*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_5_layer_call_and_return_conditional_losses_402804�
dot/PartitionedCallPartitionedCall(dense_4/StatefulPartitionedCall:output:0(dense_5/StatefulPartitionedCall:output:0*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������* 
_read_only_resource_inputs
 *-
config_proto

CPU

GPU 2J 8� *H
fCRA
?__inference_dot_layer_call_and_return_conditional_losses_402821�
dense_6/StatefulPartitionedCallStatefulPartitionedCalldot/PartitionedCall:output:0dense_6_403049dense_6_403051*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_6_layer_call_and_return_conditional_losses_402833w
IdentityIdentity(dense_6/StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:����������
NoOpNoOp^dense/StatefulPartitionedCall ^dense_1/StatefulPartitionedCall ^dense_2/StatefulPartitionedCall ^dense_3/StatefulPartitionedCall ^dense_4/StatefulPartitionedCall ^dense_5/StatefulPartitionedCall ^dense_6/StatefulPartitionedCall'^dense_features/StatefulPartitionedCall)^dense_features_1/StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*Q
_input_shapes@
>:���������:���������: : : : : : : : : : : : : : : : 2>
dense/StatefulPartitionedCalldense/StatefulPartitionedCall2B
dense_1/StatefulPartitionedCalldense_1/StatefulPartitionedCall2B
dense_2/StatefulPartitionedCalldense_2/StatefulPartitionedCall2B
dense_3/StatefulPartitionedCalldense_3/StatefulPartitionedCall2B
dense_4/StatefulPartitionedCalldense_4/StatefulPartitionedCall2B
dense_5/StatefulPartitionedCalldense_5/StatefulPartitionedCall2B
dense_6/StatefulPartitionedCalldense_6/StatefulPartitionedCall2P
&dense_features/StatefulPartitionedCall&dense_features/StatefulPartitionedCall2T
(dense_features_1/StatefulPartitionedCall(dense_features_1/StatefulPartitionedCall:&"
 
_user_specified_name403051:&"
 
_user_specified_name403049:&"
 
_user_specified_name403045:&"
 
_user_specified_name403043:&"
 
_user_specified_name403040:&"
 
_user_specified_name403038:&"
 
_user_specified_name403035:&
"
 
_user_specified_name403033:&	"
 
_user_specified_name403030:&"
 
_user_specified_name403028:&"
 
_user_specified_name403025:&"
 
_user_specified_name403023:&"
 
_user_specified_name403020:&"
 
_user_specified_name403018:&"
 
_user_specified_name403015:&"
 
_user_specified_name402928:KG
#
_output_shapes
:���������
 
_user_specified_nameuserId:L H
#
_output_shapes
:���������
!
_user_specified_name	movieId
�
�
/__inference_dense_features_layer_call_fn_403281
features_movieid
features_userid
unknown:
��

identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallfeatures_movieidfeatures_useridunknown*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������
*#
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *S
fNRL
J__inference_dense_features_layer_call_and_return_conditional_losses_402710o
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:���������
<
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*3
_input_shapes"
 :���������:���������: 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403277:TP
#
_output_shapes
:���������
)
_user_specified_namefeatures_userid:U Q
#
_output_shapes
:���������
*
_user_specified_namefeatures_movieid
�	
k
?__inference_dot_layer_call_and_return_conditional_losses_403779
inputs_0
inputs_1
identityP
ExpandDims/dimConst*
_output_shapes
: *
dtype0*
value	B :r

ExpandDims
ExpandDimsinputs_0ExpandDims/dim:output:0*
T0*,
_output_shapes
:����������R
ExpandDims_1/dimConst*
_output_shapes
: *
dtype0*
value	B :v
ExpandDims_1
ExpandDimsinputs_1ExpandDims_1/dim:output:0*
T0*,
_output_shapes
:����������y
MatMulBatchMatMulV2ExpandDims:output:0ExpandDims_1:output:0*
T0*+
_output_shapes
:���������R
ShapeShapeMatMul:output:0*
T0*
_output_shapes
::��l
SqueezeSqueezeMatMul:output:0*
T0*'
_output_shapes
:���������*
squeeze_dims
X
IdentityIdentitySqueeze:output:0*
T0*'
_output_shapes
:���������"
identityIdentity:output:0*(
_construction_contextkEagerRuntime*;
_input_shapes*
(:����������:����������:RN
(
_output_shapes
:����������
"
_user_specified_name
inputs_1:R N
(
_output_shapes
:����������
"
_user_specified_name
inputs_0
�
�
1__inference_dense_features_1_layer_call_fn_403465
features_movieid
features_userid
unknown:
��

identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallfeatures_movieidfeatures_useridunknown*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������
*#
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *U
fPRN
L__inference_dense_features_1_layer_call_and_return_conditional_losses_402623o
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:���������
<
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*3
_input_shapes"
 :���������:���������: 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403461:TP
#
_output_shapes
:���������
)
_user_specified_namefeatures_userid:U Q
#
_output_shapes
:���������
*
_user_specified_namefeatures_movieid
�
�
/__inference_dense_features_layer_call_fn_403289
features_movieid
features_userid
unknown:
��

identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallfeatures_movieidfeatures_useridunknown*
Tin
2*
Tout
2*
_collective_manager_ids
 *'
_output_shapes
:���������
*#
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *S
fNRL
J__inference_dense_features_layer_call_and_return_conditional_losses_403014o
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*'
_output_shapes
:���������
<
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*3
_input_shapes"
 :���������:���������: 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403285:TP
#
_output_shapes
:���������
)
_user_specified_namefeatures_userid:U Q
#
_output_shapes
:���������
*
_user_specified_namefeatures_movieid
�

�
C__inference_dense_1_layer_call_and_return_conditional_losses_403681

inputs0
matmul_readvariableop_resource:
 -
biasadd_readvariableop_resource: 
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpt
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes

:
 *
dtype0i
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:��������� r
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes
: *
dtype0v
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:��������� P
ReluReluBiasAdd:output:0*
T0*'
_output_shapes
:��������� a
IdentityIdentityRelu:activations:0^NoOp*
T0*'
_output_shapes
:��������� S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������
: : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:���������

 
_user_specified_nameinputs
�|
�
J__inference_dense_features_layer_call_and_return_conditional_losses_402710
features

features_1o
[movieid_embedding_movieid_embedding_weights_embedding_lookup_sparse_embedding_lookup_402671:
��

identity��TmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupk
 movieId_embedding/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
����������
movieId_embedding/ExpandDims
ExpandDimsfeatures)movieId_embedding/ExpandDims/dim:output:0*
T0*'
_output_shapes
:���������{
0movieId_embedding/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB :
����������
*movieId_embedding/to_sparse_input/NotEqualNotEqual%movieId_embedding/ExpandDims:output:09movieId_embedding/to_sparse_input/ignore_value/x:output:0*
T0*'
_output_shapes
:����������
)movieId_embedding/to_sparse_input/indicesWhere.movieId_embedding/to_sparse_input/NotEqual:z:0*'
_output_shapes
:����������
(movieId_embedding/to_sparse_input/valuesGatherNd%movieId_embedding/ExpandDims:output:01movieId_embedding/to_sparse_input/indices:index:0*
Tindices0	*
Tparams0*#
_output_shapes
:����������
-movieId_embedding/to_sparse_input/dense_shapeShape%movieId_embedding/ExpandDims:output:0*
T0*
_output_shapes
:*
out_type0	:���
movieId_embedding/valuesCast1movieId_embedding/to_sparse_input/values:output:0*

DstT0	*

SrcT0*#
_output_shapes
:����������
7movieId_embedding/movieId_embedding_weights/Slice/beginConst*
_output_shapes
:*
dtype0*
valueB: �
6movieId_embedding/movieId_embedding_weights/Slice/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
1movieId_embedding/movieId_embedding_weights/SliceSlice6movieId_embedding/to_sparse_input/dense_shape:output:0@movieId_embedding/movieId_embedding_weights/Slice/begin:output:0?movieId_embedding/movieId_embedding_weights/Slice/size:output:0*
Index0*
T0	*
_output_shapes
:{
1movieId_embedding/movieId_embedding_weights/ConstConst*
_output_shapes
:*
dtype0*
valueB: �
0movieId_embedding/movieId_embedding_weights/ProdProd:movieId_embedding/movieId_embedding_weights/Slice:output:0:movieId_embedding/movieId_embedding_weights/Const:output:0*
T0	*
_output_shapes
: ~
<movieId_embedding/movieId_embedding_weights/GatherV2/indicesConst*
_output_shapes
: *
dtype0*
value	B :{
9movieId_embedding/movieId_embedding_weights/GatherV2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
4movieId_embedding/movieId_embedding_weights/GatherV2GatherV26movieId_embedding/to_sparse_input/dense_shape:output:0EmovieId_embedding/movieId_embedding_weights/GatherV2/indices:output:0BmovieId_embedding/movieId_embedding_weights/GatherV2/axis:output:0*
Taxis0*
Tindices0*
Tparams0	*
_output_shapes
: �
2movieId_embedding/movieId_embedding_weights/Cast/xPack9movieId_embedding/movieId_embedding_weights/Prod:output:0=movieId_embedding/movieId_embedding_weights/GatherV2:output:0*
N*
T0	*
_output_shapes
:�
9movieId_embedding/movieId_embedding_weights/SparseReshapeSparseReshape1movieId_embedding/to_sparse_input/indices:index:06movieId_embedding/to_sparse_input/dense_shape:output:0;movieId_embedding/movieId_embedding_weights/Cast/x:output:0*-
_output_shapes
:���������:�
BmovieId_embedding/movieId_embedding_weights/SparseReshape/IdentityIdentitymovieId_embedding/values:y:0*
T0	*#
_output_shapes
:���������|
:movieId_embedding/movieId_embedding_weights/GreaterEqual/yConst*
_output_shapes
: *
dtype0	*
value	B	 R �
8movieId_embedding/movieId_embedding_weights/GreaterEqualGreaterEqualKmovieId_embedding/movieId_embedding_weights/SparseReshape/Identity:output:0CmovieId_embedding/movieId_embedding_weights/GreaterEqual/y:output:0*
T0	*#
_output_shapes
:����������
1movieId_embedding/movieId_embedding_weights/WhereWhere<movieId_embedding/movieId_embedding_weights/GreaterEqual:z:0*'
_output_shapes
:����������
9movieId_embedding/movieId_embedding_weights/Reshape/shapeConst*
_output_shapes
:*
dtype0*
valueB:
����������
3movieId_embedding/movieId_embedding_weights/ReshapeReshape9movieId_embedding/movieId_embedding_weights/Where:index:0BmovieId_embedding/movieId_embedding_weights/Reshape/shape:output:0*
T0	*#
_output_shapes
:���������}
;movieId_embedding/movieId_embedding_weights/GatherV2_1/axisConst*
_output_shapes
: *
dtype0*
value	B : �
6movieId_embedding/movieId_embedding_weights/GatherV2_1GatherV2JmovieId_embedding/movieId_embedding_weights/SparseReshape:output_indices:0<movieId_embedding/movieId_embedding_weights/Reshape:output:0DmovieId_embedding/movieId_embedding_weights/GatherV2_1/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*'
_output_shapes
:���������}
;movieId_embedding/movieId_embedding_weights/GatherV2_2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
6movieId_embedding/movieId_embedding_weights/GatherV2_2GatherV2KmovieId_embedding/movieId_embedding_weights/SparseReshape/Identity:output:0<movieId_embedding/movieId_embedding_weights/Reshape:output:0DmovieId_embedding/movieId_embedding_weights/GatherV2_2/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*#
_output_shapes
:����������
4movieId_embedding/movieId_embedding_weights/IdentityIdentityHmovieId_embedding/movieId_embedding_weights/SparseReshape:output_shape:0*
T0	*
_output_shapes
:�
EmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/ConstConst*
_output_shapes
: *
dtype0	*
value	B	 R �
SmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRowsSparseFillEmptyRows?movieId_embedding/movieId_embedding_weights/GatherV2_1:output:0?movieId_embedding/movieId_embedding_weights/GatherV2_2:output:0=movieId_embedding/movieId_embedding_weights/Identity:output:0NmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/Const:output:0*
T0	*T
_output_shapesB
@:���������:���������:���������:����������
WmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB"        �
YmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB"       �
YmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB"      �
QmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_sliceStridedSlicedmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_indices:0`movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack:output:0bmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1:output:0bmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2:output:0*
Index0*
T0	*#
_output_shapes
:���������*

begin_mask*
end_mask*
shrink_axis_mask�
JmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/UniqueUniquecmovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_values:0*
T0	*2
_output_shapes 
:���������:����������
TmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupResourceGather[movieid_embedding_movieid_embedding_weights_embedding_lookup_sparse_embedding_lookup_402671NmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/Unique:y:0*
Tindices0	*n
_classd
b`loc:@movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/402671*'
_output_shapes
:���������
*
dtype0�
]movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/IdentityIdentity]movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup:output:0*
T0*'
_output_shapes
:���������
�
CmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparseSparseSegmentMeanfmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/Identity:output:0PmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/Unique:idx:0ZmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice:output:0*
Tsegmentids0	*
T0*'
_output_shapes
:���������
�
;movieId_embedding/movieId_embedding_weights/Reshape_1/shapeConst*
_output_shapes
:*
dtype0*
valueB"����   �
5movieId_embedding/movieId_embedding_weights/Reshape_1ReshapeimovieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:empty_row_indicator:0DmovieId_embedding/movieId_embedding_weights/Reshape_1/shape:output:0*
T0
*'
_output_shapes
:����������
1movieId_embedding/movieId_embedding_weights/ShapeShapeLmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*
_output_shapes
::���
?movieId_embedding/movieId_embedding_weights/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB:�
AmovieId_embedding/movieId_embedding_weights/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:�
AmovieId_embedding/movieId_embedding_weights/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
9movieId_embedding/movieId_embedding_weights/strided_sliceStridedSlice:movieId_embedding/movieId_embedding_weights/Shape:output:0HmovieId_embedding/movieId_embedding_weights/strided_slice/stack:output:0JmovieId_embedding/movieId_embedding_weights/strided_slice/stack_1:output:0JmovieId_embedding/movieId_embedding_weights/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_masku
3movieId_embedding/movieId_embedding_weights/stack/0Const*
_output_shapes
: *
dtype0*
value	B :�
1movieId_embedding/movieId_embedding_weights/stackPack<movieId_embedding/movieId_embedding_weights/stack/0:output:0BmovieId_embedding/movieId_embedding_weights/strided_slice:output:0*
N*
T0*
_output_shapes
:�
0movieId_embedding/movieId_embedding_weights/TileTile>movieId_embedding/movieId_embedding_weights/Reshape_1:output:0:movieId_embedding/movieId_embedding_weights/stack:output:0*
T0
*'
_output_shapes
:���������
�
6movieId_embedding/movieId_embedding_weights/zeros_like	ZerosLikeLmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
+movieId_embedding/movieId_embedding_weightsSelect9movieId_embedding/movieId_embedding_weights/Tile:output:0:movieId_embedding/movieId_embedding_weights/zeros_like:y:0LmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
2movieId_embedding/movieId_embedding_weights/Cast_1Cast6movieId_embedding/to_sparse_input/dense_shape:output:0*

DstT0*

SrcT0	*
_output_shapes
:�
9movieId_embedding/movieId_embedding_weights/Slice_1/beginConst*
_output_shapes
:*
dtype0*
valueB: �
8movieId_embedding/movieId_embedding_weights/Slice_1/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
3movieId_embedding/movieId_embedding_weights/Slice_1Slice6movieId_embedding/movieId_embedding_weights/Cast_1:y:0BmovieId_embedding/movieId_embedding_weights/Slice_1/begin:output:0AmovieId_embedding/movieId_embedding_weights/Slice_1/size:output:0*
Index0*
T0*
_output_shapes
:�
3movieId_embedding/movieId_embedding_weights/Shape_1Shape4movieId_embedding/movieId_embedding_weights:output:0*
T0*
_output_shapes
::���
9movieId_embedding/movieId_embedding_weights/Slice_2/beginConst*
_output_shapes
:*
dtype0*
valueB:�
8movieId_embedding/movieId_embedding_weights/Slice_2/sizeConst*
_output_shapes
:*
dtype0*
valueB:
����������
3movieId_embedding/movieId_embedding_weights/Slice_2Slice<movieId_embedding/movieId_embedding_weights/Shape_1:output:0BmovieId_embedding/movieId_embedding_weights/Slice_2/begin:output:0AmovieId_embedding/movieId_embedding_weights/Slice_2/size:output:0*
Index0*
T0*
_output_shapes
:y
7movieId_embedding/movieId_embedding_weights/concat/axisConst*
_output_shapes
: *
dtype0*
value	B : �
2movieId_embedding/movieId_embedding_weights/concatConcatV2<movieId_embedding/movieId_embedding_weights/Slice_1:output:0<movieId_embedding/movieId_embedding_weights/Slice_2:output:0@movieId_embedding/movieId_embedding_weights/concat/axis:output:0*
N*
T0*
_output_shapes
:�
5movieId_embedding/movieId_embedding_weights/Reshape_2Reshape4movieId_embedding/movieId_embedding_weights:output:0;movieId_embedding/movieId_embedding_weights/concat:output:0*
T0*'
_output_shapes
:���������
�
movieId_embedding/ShapeShape>movieId_embedding/movieId_embedding_weights/Reshape_2:output:0*
T0*
_output_shapes
::��o
%movieId_embedding/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: q
'movieId_embedding/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:q
'movieId_embedding/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
movieId_embedding/strided_sliceStridedSlice movieId_embedding/Shape:output:0.movieId_embedding/strided_slice/stack:output:00movieId_embedding/strided_slice/stack_1:output:00movieId_embedding/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_maskc
!movieId_embedding/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
�
movieId_embedding/Reshape/shapePack(movieId_embedding/strided_slice:output:0*movieId_embedding/Reshape/shape/1:output:0*
N*
T0*
_output_shapes
:�
movieId_embedding/ReshapeReshape>movieId_embedding/movieId_embedding_weights/Reshape_2:output:0(movieId_embedding/Reshape/shape:output:0*
T0*'
_output_shapes
:���������
\
concat/concat_dimConst*
_output_shapes
: *
dtype0*
valueB :
���������o
concat/concatIdentity"movieId_embedding/Reshape:output:0*
T0*'
_output_shapes
:���������
e
IdentityIdentityconcat/concat:output:0^NoOp*
T0*'
_output_shapes
:���������
y
NoOpNoOpU^movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*3
_input_shapes"
 :���������:���������: 2�
TmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupTmovieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup:&"
 
_user_specified_name402671:MI
#
_output_shapes
:���������
"
_user_specified_name
features:M I
#
_output_shapes
:���������
"
_user_specified_name
features
�

�
C__inference_dense_1_layer_call_and_return_conditional_losses_402724

inputs0
matmul_readvariableop_resource:
 -
biasadd_readvariableop_resource: 
identity��BiasAdd/ReadVariableOp�MatMul/ReadVariableOpt
MatMul/ReadVariableOpReadVariableOpmatmul_readvariableop_resource*
_output_shapes

:
 *
dtype0i
MatMulMatMulinputsMatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:��������� r
BiasAdd/ReadVariableOpReadVariableOpbiasadd_readvariableop_resource*
_output_shapes
: *
dtype0v
BiasAddBiasAddMatMul:product:0BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:��������� P
ReluReluBiasAdd:output:0*
T0*'
_output_shapes
:��������� a
IdentityIdentityRelu:activations:0^NoOp*
T0*'
_output_shapes
:��������� S
NoOpNoOp^BiasAdd/ReadVariableOp^MatMul/ReadVariableOp*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������
: : 20
BiasAdd/ReadVariableOpBiasAdd/ReadVariableOp2.
MatMul/ReadVariableOpMatMul/ReadVariableOp:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:O K
'
_output_shapes
:���������

 
_user_specified_nameinputs
��
�
!__inference__wrapped_model_402536
movieid

userid�
pmodel_dense_features_1_userid_embedding_userid_embedding_weights_embedding_lookup_sparse_embedding_lookup_402362:
��
�
pmodel_dense_features_movieid_embedding_movieid_embedding_weights_embedding_lookup_sparse_embedding_lookup_402441:
��
>
,model_dense_1_matmul_readvariableop_resource:
 ;
-model_dense_1_biasadd_readvariableop_resource: <
*model_dense_matmul_readvariableop_resource:
 9
+model_dense_biasadd_readvariableop_resource: >
,model_dense_3_matmul_readvariableop_resource: @;
-model_dense_3_biasadd_readvariableop_resource:@>
,model_dense_2_matmul_readvariableop_resource: @;
-model_dense_2_biasadd_readvariableop_resource:@?
,model_dense_4_matmul_readvariableop_resource:	@�<
-model_dense_4_biasadd_readvariableop_resource:	�?
,model_dense_5_matmul_readvariableop_resource:	@�<
-model_dense_5_biasadd_readvariableop_resource:	�>
,model_dense_6_matmul_readvariableop_resource:;
-model_dense_6_biasadd_readvariableop_resource:
identity��"model/dense/BiasAdd/ReadVariableOp�!model/dense/MatMul/ReadVariableOp�$model/dense_1/BiasAdd/ReadVariableOp�#model/dense_1/MatMul/ReadVariableOp�$model/dense_2/BiasAdd/ReadVariableOp�#model/dense_2/MatMul/ReadVariableOp�$model/dense_3/BiasAdd/ReadVariableOp�#model/dense_3/MatMul/ReadVariableOp�$model/dense_4/BiasAdd/ReadVariableOp�#model/dense_4/MatMul/ReadVariableOp�$model/dense_5/BiasAdd/ReadVariableOp�#model/dense_5/MatMul/ReadVariableOp�$model/dense_6/BiasAdd/ReadVariableOp�#model/dense_6/MatMul/ReadVariableOp�imodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup�imodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup�
6model/dense_features_1/userId_embedding/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
����������
2model/dense_features_1/userId_embedding/ExpandDims
ExpandDimsuserid?model/dense_features_1/userId_embedding/ExpandDims/dim:output:0*
T0*'
_output_shapes
:����������
Fmodel/dense_features_1/userId_embedding/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB :
����������
@model/dense_features_1/userId_embedding/to_sparse_input/NotEqualNotEqual;model/dense_features_1/userId_embedding/ExpandDims:output:0Omodel/dense_features_1/userId_embedding/to_sparse_input/ignore_value/x:output:0*
T0*'
_output_shapes
:����������
?model/dense_features_1/userId_embedding/to_sparse_input/indicesWhereDmodel/dense_features_1/userId_embedding/to_sparse_input/NotEqual:z:0*'
_output_shapes
:����������
>model/dense_features_1/userId_embedding/to_sparse_input/valuesGatherNd;model/dense_features_1/userId_embedding/ExpandDims:output:0Gmodel/dense_features_1/userId_embedding/to_sparse_input/indices:index:0*
Tindices0	*
Tparams0*#
_output_shapes
:����������
Cmodel/dense_features_1/userId_embedding/to_sparse_input/dense_shapeShape;model/dense_features_1/userId_embedding/ExpandDims:output:0*
T0*
_output_shapes
:*
out_type0	:���
.model/dense_features_1/userId_embedding/valuesCastGmodel/dense_features_1/userId_embedding/to_sparse_input/values:output:0*

DstT0	*

SrcT0*#
_output_shapes
:����������
Lmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice/beginConst*
_output_shapes
:*
dtype0*
valueB: �
Kmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
Fmodel/dense_features_1/userId_embedding/userId_embedding_weights/SliceSliceLmodel/dense_features_1/userId_embedding/to_sparse_input/dense_shape:output:0Umodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice/begin:output:0Tmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice/size:output:0*
Index0*
T0	*
_output_shapes
:�
Fmodel/dense_features_1/userId_embedding/userId_embedding_weights/ConstConst*
_output_shapes
:*
dtype0*
valueB: �
Emodel/dense_features_1/userId_embedding/userId_embedding_weights/ProdProdOmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice:output:0Omodel/dense_features_1/userId_embedding/userId_embedding_weights/Const:output:0*
T0	*
_output_shapes
: �
Qmodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2/indicesConst*
_output_shapes
: *
dtype0*
value	B :�
Nmodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
Imodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2GatherV2Lmodel/dense_features_1/userId_embedding/to_sparse_input/dense_shape:output:0Zmodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2/indices:output:0Wmodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2/axis:output:0*
Taxis0*
Tindices0*
Tparams0	*
_output_shapes
: �
Gmodel/dense_features_1/userId_embedding/userId_embedding_weights/Cast/xPackNmodel/dense_features_1/userId_embedding/userId_embedding_weights/Prod:output:0Rmodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2:output:0*
N*
T0	*
_output_shapes
:�
Nmodel/dense_features_1/userId_embedding/userId_embedding_weights/SparseReshapeSparseReshapeGmodel/dense_features_1/userId_embedding/to_sparse_input/indices:index:0Lmodel/dense_features_1/userId_embedding/to_sparse_input/dense_shape:output:0Pmodel/dense_features_1/userId_embedding/userId_embedding_weights/Cast/x:output:0*-
_output_shapes
:���������:�
Wmodel/dense_features_1/userId_embedding/userId_embedding_weights/SparseReshape/IdentityIdentity2model/dense_features_1/userId_embedding/values:y:0*
T0	*#
_output_shapes
:����������
Omodel/dense_features_1/userId_embedding/userId_embedding_weights/GreaterEqual/yConst*
_output_shapes
: *
dtype0	*
value	B	 R �
Mmodel/dense_features_1/userId_embedding/userId_embedding_weights/GreaterEqualGreaterEqual`model/dense_features_1/userId_embedding/userId_embedding_weights/SparseReshape/Identity:output:0Xmodel/dense_features_1/userId_embedding/userId_embedding_weights/GreaterEqual/y:output:0*
T0	*#
_output_shapes
:����������
Fmodel/dense_features_1/userId_embedding/userId_embedding_weights/WhereWhereQmodel/dense_features_1/userId_embedding/userId_embedding_weights/GreaterEqual:z:0*'
_output_shapes
:����������
Nmodel/dense_features_1/userId_embedding/userId_embedding_weights/Reshape/shapeConst*
_output_shapes
:*
dtype0*
valueB:
����������
Hmodel/dense_features_1/userId_embedding/userId_embedding_weights/ReshapeReshapeNmodel/dense_features_1/userId_embedding/userId_embedding_weights/Where:index:0Wmodel/dense_features_1/userId_embedding/userId_embedding_weights/Reshape/shape:output:0*
T0	*#
_output_shapes
:����������
Pmodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2_1/axisConst*
_output_shapes
: *
dtype0*
value	B : �
Kmodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2_1GatherV2_model/dense_features_1/userId_embedding/userId_embedding_weights/SparseReshape:output_indices:0Qmodel/dense_features_1/userId_embedding/userId_embedding_weights/Reshape:output:0Ymodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2_1/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*'
_output_shapes
:����������
Pmodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2_2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
Kmodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2_2GatherV2`model/dense_features_1/userId_embedding/userId_embedding_weights/SparseReshape/Identity:output:0Qmodel/dense_features_1/userId_embedding/userId_embedding_weights/Reshape:output:0Ymodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2_2/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*#
_output_shapes
:����������
Imodel/dense_features_1/userId_embedding/userId_embedding_weights/IdentityIdentity]model/dense_features_1/userId_embedding/userId_embedding_weights/SparseReshape:output_shape:0*
T0	*
_output_shapes
:�
Zmodel/dense_features_1/userId_embedding/userId_embedding_weights/SparseFillEmptyRows/ConstConst*
_output_shapes
: *
dtype0	*
value	B	 R �
hmodel/dense_features_1/userId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRowsSparseFillEmptyRowsTmodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2_1:output:0Tmodel/dense_features_1/userId_embedding/userId_embedding_weights/GatherV2_2:output:0Rmodel/dense_features_1/userId_embedding/userId_embedding_weights/Identity:output:0cmodel/dense_features_1/userId_embedding/userId_embedding_weights/SparseFillEmptyRows/Const:output:0*
T0	*T
_output_shapesB
@:���������:���������:���������:����������
lmodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB"        �
nmodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB"       �
nmodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB"      �
fmodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_sliceStridedSliceymodel/dense_features_1/userId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_indices:0umodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack:output:0wmodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1:output:0wmodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2:output:0*
Index0*
T0	*#
_output_shapes
:���������*

begin_mask*
end_mask*
shrink_axis_mask�
_model/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/UniqueUniquexmodel/dense_features_1/userId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_values:0*
T0	*2
_output_shapes 
:���������:����������
imodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupResourceGatherpmodel_dense_features_1_userid_embedding_userid_embedding_weights_embedding_lookup_sparse_embedding_lookup_402362cmodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/Unique:y:0*
Tindices0	*�
_classy
wuloc:@model/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/402362*'
_output_shapes
:���������
*
dtype0�
rmodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/IdentityIdentityrmodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup:output:0*
T0*'
_output_shapes
:���������
�
Xmodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparseSparseSegmentMean{model/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup/Identity:output:0emodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/Unique:idx:0omodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/strided_slice:output:0*
Tsegmentids0	*
T0*'
_output_shapes
:���������
�
Pmodel/dense_features_1/userId_embedding/userId_embedding_weights/Reshape_1/shapeConst*
_output_shapes
:*
dtype0*
valueB"����   �
Jmodel/dense_features_1/userId_embedding/userId_embedding_weights/Reshape_1Reshape~model/dense_features_1/userId_embedding/userId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:empty_row_indicator:0Ymodel/dense_features_1/userId_embedding/userId_embedding_weights/Reshape_1/shape:output:0*
T0
*'
_output_shapes
:����������
Fmodel/dense_features_1/userId_embedding/userId_embedding_weights/ShapeShapeamodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*
_output_shapes
::���
Tmodel/dense_features_1/userId_embedding/userId_embedding_weights/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB:�
Vmodel/dense_features_1/userId_embedding/userId_embedding_weights/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:�
Vmodel/dense_features_1/userId_embedding/userId_embedding_weights/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
Nmodel/dense_features_1/userId_embedding/userId_embedding_weights/strided_sliceStridedSliceOmodel/dense_features_1/userId_embedding/userId_embedding_weights/Shape:output:0]model/dense_features_1/userId_embedding/userId_embedding_weights/strided_slice/stack:output:0_model/dense_features_1/userId_embedding/userId_embedding_weights/strided_slice/stack_1:output:0_model/dense_features_1/userId_embedding/userId_embedding_weights/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_mask�
Hmodel/dense_features_1/userId_embedding/userId_embedding_weights/stack/0Const*
_output_shapes
: *
dtype0*
value	B :�
Fmodel/dense_features_1/userId_embedding/userId_embedding_weights/stackPackQmodel/dense_features_1/userId_embedding/userId_embedding_weights/stack/0:output:0Wmodel/dense_features_1/userId_embedding/userId_embedding_weights/strided_slice:output:0*
N*
T0*
_output_shapes
:�
Emodel/dense_features_1/userId_embedding/userId_embedding_weights/TileTileSmodel/dense_features_1/userId_embedding/userId_embedding_weights/Reshape_1:output:0Omodel/dense_features_1/userId_embedding/userId_embedding_weights/stack:output:0*
T0
*'
_output_shapes
:���������
�
Kmodel/dense_features_1/userId_embedding/userId_embedding_weights/zeros_like	ZerosLikeamodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
@model/dense_features_1/userId_embedding/userId_embedding_weightsSelectNmodel/dense_features_1/userId_embedding/userId_embedding_weights/Tile:output:0Omodel/dense_features_1/userId_embedding/userId_embedding_weights/zeros_like:y:0amodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
Gmodel/dense_features_1/userId_embedding/userId_embedding_weights/Cast_1CastLmodel/dense_features_1/userId_embedding/to_sparse_input/dense_shape:output:0*

DstT0*

SrcT0	*
_output_shapes
:�
Nmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice_1/beginConst*
_output_shapes
:*
dtype0*
valueB: �
Mmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice_1/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
Hmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice_1SliceKmodel/dense_features_1/userId_embedding/userId_embedding_weights/Cast_1:y:0Wmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice_1/begin:output:0Vmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice_1/size:output:0*
Index0*
T0*
_output_shapes
:�
Hmodel/dense_features_1/userId_embedding/userId_embedding_weights/Shape_1ShapeImodel/dense_features_1/userId_embedding/userId_embedding_weights:output:0*
T0*
_output_shapes
::���
Nmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice_2/beginConst*
_output_shapes
:*
dtype0*
valueB:�
Mmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice_2/sizeConst*
_output_shapes
:*
dtype0*
valueB:
����������
Hmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice_2SliceQmodel/dense_features_1/userId_embedding/userId_embedding_weights/Shape_1:output:0Wmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice_2/begin:output:0Vmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice_2/size:output:0*
Index0*
T0*
_output_shapes
:�
Lmodel/dense_features_1/userId_embedding/userId_embedding_weights/concat/axisConst*
_output_shapes
: *
dtype0*
value	B : �
Gmodel/dense_features_1/userId_embedding/userId_embedding_weights/concatConcatV2Qmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice_1:output:0Qmodel/dense_features_1/userId_embedding/userId_embedding_weights/Slice_2:output:0Umodel/dense_features_1/userId_embedding/userId_embedding_weights/concat/axis:output:0*
N*
T0*
_output_shapes
:�
Jmodel/dense_features_1/userId_embedding/userId_embedding_weights/Reshape_2ReshapeImodel/dense_features_1/userId_embedding/userId_embedding_weights:output:0Pmodel/dense_features_1/userId_embedding/userId_embedding_weights/concat:output:0*
T0*'
_output_shapes
:���������
�
-model/dense_features_1/userId_embedding/ShapeShapeSmodel/dense_features_1/userId_embedding/userId_embedding_weights/Reshape_2:output:0*
T0*
_output_shapes
::���
;model/dense_features_1/userId_embedding/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: �
=model/dense_features_1/userId_embedding/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:�
=model/dense_features_1/userId_embedding/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
5model/dense_features_1/userId_embedding/strided_sliceStridedSlice6model/dense_features_1/userId_embedding/Shape:output:0Dmodel/dense_features_1/userId_embedding/strided_slice/stack:output:0Fmodel/dense_features_1/userId_embedding/strided_slice/stack_1:output:0Fmodel/dense_features_1/userId_embedding/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_masky
7model/dense_features_1/userId_embedding/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
�
5model/dense_features_1/userId_embedding/Reshape/shapePack>model/dense_features_1/userId_embedding/strided_slice:output:0@model/dense_features_1/userId_embedding/Reshape/shape/1:output:0*
N*
T0*
_output_shapes
:�
/model/dense_features_1/userId_embedding/ReshapeReshapeSmodel/dense_features_1/userId_embedding/userId_embedding_weights/Reshape_2:output:0>model/dense_features_1/userId_embedding/Reshape/shape:output:0*
T0*'
_output_shapes
:���������
s
(model/dense_features_1/concat/concat_dimConst*
_output_shapes
: *
dtype0*
valueB :
����������
$model/dense_features_1/concat/concatIdentity8model/dense_features_1/userId_embedding/Reshape:output:0*
T0*'
_output_shapes
:���������
�
5model/dense_features/movieId_embedding/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
����������
1model/dense_features/movieId_embedding/ExpandDims
ExpandDimsmovieid>model/dense_features/movieId_embedding/ExpandDims/dim:output:0*
T0*'
_output_shapes
:����������
Emodel/dense_features/movieId_embedding/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB :
����������
?model/dense_features/movieId_embedding/to_sparse_input/NotEqualNotEqual:model/dense_features/movieId_embedding/ExpandDims:output:0Nmodel/dense_features/movieId_embedding/to_sparse_input/ignore_value/x:output:0*
T0*'
_output_shapes
:����������
>model/dense_features/movieId_embedding/to_sparse_input/indicesWhereCmodel/dense_features/movieId_embedding/to_sparse_input/NotEqual:z:0*'
_output_shapes
:����������
=model/dense_features/movieId_embedding/to_sparse_input/valuesGatherNd:model/dense_features/movieId_embedding/ExpandDims:output:0Fmodel/dense_features/movieId_embedding/to_sparse_input/indices:index:0*
Tindices0	*
Tparams0*#
_output_shapes
:����������
Bmodel/dense_features/movieId_embedding/to_sparse_input/dense_shapeShape:model/dense_features/movieId_embedding/ExpandDims:output:0*
T0*
_output_shapes
:*
out_type0	:���
-model/dense_features/movieId_embedding/valuesCastFmodel/dense_features/movieId_embedding/to_sparse_input/values:output:0*

DstT0	*

SrcT0*#
_output_shapes
:����������
Lmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice/beginConst*
_output_shapes
:*
dtype0*
valueB: �
Kmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
Fmodel/dense_features/movieId_embedding/movieId_embedding_weights/SliceSliceKmodel/dense_features/movieId_embedding/to_sparse_input/dense_shape:output:0Umodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice/begin:output:0Tmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice/size:output:0*
Index0*
T0	*
_output_shapes
:�
Fmodel/dense_features/movieId_embedding/movieId_embedding_weights/ConstConst*
_output_shapes
:*
dtype0*
valueB: �
Emodel/dense_features/movieId_embedding/movieId_embedding_weights/ProdProdOmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice:output:0Omodel/dense_features/movieId_embedding/movieId_embedding_weights/Const:output:0*
T0	*
_output_shapes
: �
Qmodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2/indicesConst*
_output_shapes
: *
dtype0*
value	B :�
Nmodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
Imodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2GatherV2Kmodel/dense_features/movieId_embedding/to_sparse_input/dense_shape:output:0Zmodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2/indices:output:0Wmodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2/axis:output:0*
Taxis0*
Tindices0*
Tparams0	*
_output_shapes
: �
Gmodel/dense_features/movieId_embedding/movieId_embedding_weights/Cast/xPackNmodel/dense_features/movieId_embedding/movieId_embedding_weights/Prod:output:0Rmodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2:output:0*
N*
T0	*
_output_shapes
:�
Nmodel/dense_features/movieId_embedding/movieId_embedding_weights/SparseReshapeSparseReshapeFmodel/dense_features/movieId_embedding/to_sparse_input/indices:index:0Kmodel/dense_features/movieId_embedding/to_sparse_input/dense_shape:output:0Pmodel/dense_features/movieId_embedding/movieId_embedding_weights/Cast/x:output:0*-
_output_shapes
:���������:�
Wmodel/dense_features/movieId_embedding/movieId_embedding_weights/SparseReshape/IdentityIdentity1model/dense_features/movieId_embedding/values:y:0*
T0	*#
_output_shapes
:����������
Omodel/dense_features/movieId_embedding/movieId_embedding_weights/GreaterEqual/yConst*
_output_shapes
: *
dtype0	*
value	B	 R �
Mmodel/dense_features/movieId_embedding/movieId_embedding_weights/GreaterEqualGreaterEqual`model/dense_features/movieId_embedding/movieId_embedding_weights/SparseReshape/Identity:output:0Xmodel/dense_features/movieId_embedding/movieId_embedding_weights/GreaterEqual/y:output:0*
T0	*#
_output_shapes
:����������
Fmodel/dense_features/movieId_embedding/movieId_embedding_weights/WhereWhereQmodel/dense_features/movieId_embedding/movieId_embedding_weights/GreaterEqual:z:0*'
_output_shapes
:����������
Nmodel/dense_features/movieId_embedding/movieId_embedding_weights/Reshape/shapeConst*
_output_shapes
:*
dtype0*
valueB:
����������
Hmodel/dense_features/movieId_embedding/movieId_embedding_weights/ReshapeReshapeNmodel/dense_features/movieId_embedding/movieId_embedding_weights/Where:index:0Wmodel/dense_features/movieId_embedding/movieId_embedding_weights/Reshape/shape:output:0*
T0	*#
_output_shapes
:����������
Pmodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2_1/axisConst*
_output_shapes
: *
dtype0*
value	B : �
Kmodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2_1GatherV2_model/dense_features/movieId_embedding/movieId_embedding_weights/SparseReshape:output_indices:0Qmodel/dense_features/movieId_embedding/movieId_embedding_weights/Reshape:output:0Ymodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2_1/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*'
_output_shapes
:����������
Pmodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2_2/axisConst*
_output_shapes
: *
dtype0*
value	B : �
Kmodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2_2GatherV2`model/dense_features/movieId_embedding/movieId_embedding_weights/SparseReshape/Identity:output:0Qmodel/dense_features/movieId_embedding/movieId_embedding_weights/Reshape:output:0Ymodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2_2/axis:output:0*
Taxis0*
Tindices0	*
Tparams0	*#
_output_shapes
:����������
Imodel/dense_features/movieId_embedding/movieId_embedding_weights/IdentityIdentity]model/dense_features/movieId_embedding/movieId_embedding_weights/SparseReshape:output_shape:0*
T0	*
_output_shapes
:�
Zmodel/dense_features/movieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/ConstConst*
_output_shapes
: *
dtype0	*
value	B	 R �
hmodel/dense_features/movieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRowsSparseFillEmptyRowsTmodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2_1:output:0Tmodel/dense_features/movieId_embedding/movieId_embedding_weights/GatherV2_2:output:0Rmodel/dense_features/movieId_embedding/movieId_embedding_weights/Identity:output:0cmodel/dense_features/movieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/Const:output:0*
T0	*T
_output_shapesB
@:���������:���������:���������:����������
lmodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB"        �
nmodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB"       �
nmodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB"      �
fmodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_sliceStridedSliceymodel/dense_features/movieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_indices:0umodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack:output:0wmodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_1:output:0wmodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice/stack_2:output:0*
Index0*
T0	*#
_output_shapes
:���������*

begin_mask*
end_mask*
shrink_axis_mask�
_model/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/UniqueUniquexmodel/dense_features/movieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:output_values:0*
T0	*2
_output_shapes 
:���������:����������
imodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupResourceGatherpmodel_dense_features_movieid_embedding_movieid_embedding_weights_embedding_lookup_sparse_embedding_lookup_402441cmodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/Unique:y:0*
Tindices0	*�
_classy
wuloc:@model/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/402441*'
_output_shapes
:���������
*
dtype0�
rmodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/IdentityIdentityrmodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup:output:0*
T0*'
_output_shapes
:���������
�
Xmodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparseSparseSegmentMean{model/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup/Identity:output:0emodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/Unique:idx:0omodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/strided_slice:output:0*
Tsegmentids0	*
T0*'
_output_shapes
:���������
�
Pmodel/dense_features/movieId_embedding/movieId_embedding_weights/Reshape_1/shapeConst*
_output_shapes
:*
dtype0*
valueB"����   �
Jmodel/dense_features/movieId_embedding/movieId_embedding_weights/Reshape_1Reshape~model/dense_features/movieId_embedding/movieId_embedding_weights/SparseFillEmptyRows/SparseFillEmptyRows:empty_row_indicator:0Ymodel/dense_features/movieId_embedding/movieId_embedding_weights/Reshape_1/shape:output:0*
T0
*'
_output_shapes
:����������
Fmodel/dense_features/movieId_embedding/movieId_embedding_weights/ShapeShapeamodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*
_output_shapes
::���
Tmodel/dense_features/movieId_embedding/movieId_embedding_weights/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB:�
Vmodel/dense_features/movieId_embedding/movieId_embedding_weights/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:�
Vmodel/dense_features/movieId_embedding/movieId_embedding_weights/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
Nmodel/dense_features/movieId_embedding/movieId_embedding_weights/strided_sliceStridedSliceOmodel/dense_features/movieId_embedding/movieId_embedding_weights/Shape:output:0]model/dense_features/movieId_embedding/movieId_embedding_weights/strided_slice/stack:output:0_model/dense_features/movieId_embedding/movieId_embedding_weights/strided_slice/stack_1:output:0_model/dense_features/movieId_embedding/movieId_embedding_weights/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_mask�
Hmodel/dense_features/movieId_embedding/movieId_embedding_weights/stack/0Const*
_output_shapes
: *
dtype0*
value	B :�
Fmodel/dense_features/movieId_embedding/movieId_embedding_weights/stackPackQmodel/dense_features/movieId_embedding/movieId_embedding_weights/stack/0:output:0Wmodel/dense_features/movieId_embedding/movieId_embedding_weights/strided_slice:output:0*
N*
T0*
_output_shapes
:�
Emodel/dense_features/movieId_embedding/movieId_embedding_weights/TileTileSmodel/dense_features/movieId_embedding/movieId_embedding_weights/Reshape_1:output:0Omodel/dense_features/movieId_embedding/movieId_embedding_weights/stack:output:0*
T0
*'
_output_shapes
:���������
�
Kmodel/dense_features/movieId_embedding/movieId_embedding_weights/zeros_like	ZerosLikeamodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
@model/dense_features/movieId_embedding/movieId_embedding_weightsSelectNmodel/dense_features/movieId_embedding/movieId_embedding_weights/Tile:output:0Omodel/dense_features/movieId_embedding/movieId_embedding_weights/zeros_like:y:0amodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse:output:0*
T0*'
_output_shapes
:���������
�
Gmodel/dense_features/movieId_embedding/movieId_embedding_weights/Cast_1CastKmodel/dense_features/movieId_embedding/to_sparse_input/dense_shape:output:0*

DstT0*

SrcT0	*
_output_shapes
:�
Nmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice_1/beginConst*
_output_shapes
:*
dtype0*
valueB: �
Mmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice_1/sizeConst*
_output_shapes
:*
dtype0*
valueB:�
Hmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice_1SliceKmodel/dense_features/movieId_embedding/movieId_embedding_weights/Cast_1:y:0Wmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice_1/begin:output:0Vmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice_1/size:output:0*
Index0*
T0*
_output_shapes
:�
Hmodel/dense_features/movieId_embedding/movieId_embedding_weights/Shape_1ShapeImodel/dense_features/movieId_embedding/movieId_embedding_weights:output:0*
T0*
_output_shapes
::���
Nmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice_2/beginConst*
_output_shapes
:*
dtype0*
valueB:�
Mmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice_2/sizeConst*
_output_shapes
:*
dtype0*
valueB:
����������
Hmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice_2SliceQmodel/dense_features/movieId_embedding/movieId_embedding_weights/Shape_1:output:0Wmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice_2/begin:output:0Vmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice_2/size:output:0*
Index0*
T0*
_output_shapes
:�
Lmodel/dense_features/movieId_embedding/movieId_embedding_weights/concat/axisConst*
_output_shapes
: *
dtype0*
value	B : �
Gmodel/dense_features/movieId_embedding/movieId_embedding_weights/concatConcatV2Qmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice_1:output:0Qmodel/dense_features/movieId_embedding/movieId_embedding_weights/Slice_2:output:0Umodel/dense_features/movieId_embedding/movieId_embedding_weights/concat/axis:output:0*
N*
T0*
_output_shapes
:�
Jmodel/dense_features/movieId_embedding/movieId_embedding_weights/Reshape_2ReshapeImodel/dense_features/movieId_embedding/movieId_embedding_weights:output:0Pmodel/dense_features/movieId_embedding/movieId_embedding_weights/concat:output:0*
T0*'
_output_shapes
:���������
�
,model/dense_features/movieId_embedding/ShapeShapeSmodel/dense_features/movieId_embedding/movieId_embedding_weights/Reshape_2:output:0*
T0*
_output_shapes
::���
:model/dense_features/movieId_embedding/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: �
<model/dense_features/movieId_embedding/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:�
<model/dense_features/movieId_embedding/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:�
4model/dense_features/movieId_embedding/strided_sliceStridedSlice5model/dense_features/movieId_embedding/Shape:output:0Cmodel/dense_features/movieId_embedding/strided_slice/stack:output:0Emodel/dense_features/movieId_embedding/strided_slice/stack_1:output:0Emodel/dense_features/movieId_embedding/strided_slice/stack_2:output:0*
Index0*
T0*
_output_shapes
: *
shrink_axis_maskx
6model/dense_features/movieId_embedding/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
�
4model/dense_features/movieId_embedding/Reshape/shapePack=model/dense_features/movieId_embedding/strided_slice:output:0?model/dense_features/movieId_embedding/Reshape/shape/1:output:0*
N*
T0*
_output_shapes
:�
.model/dense_features/movieId_embedding/ReshapeReshapeSmodel/dense_features/movieId_embedding/movieId_embedding_weights/Reshape_2:output:0=model/dense_features/movieId_embedding/Reshape/shape:output:0*
T0*'
_output_shapes
:���������
q
&model/dense_features/concat/concat_dimConst*
_output_shapes
: *
dtype0*
valueB :
����������
"model/dense_features/concat/concatIdentity7model/dense_features/movieId_embedding/Reshape:output:0*
T0*'
_output_shapes
:���������
�
#model/dense_1/MatMul/ReadVariableOpReadVariableOp,model_dense_1_matmul_readvariableop_resource*
_output_shapes

:
 *
dtype0�
model/dense_1/MatMulMatMul-model/dense_features_1/concat/concat:output:0+model/dense_1/MatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:��������� �
$model/dense_1/BiasAdd/ReadVariableOpReadVariableOp-model_dense_1_biasadd_readvariableop_resource*
_output_shapes
: *
dtype0�
model/dense_1/BiasAddBiasAddmodel/dense_1/MatMul:product:0,model/dense_1/BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:��������� l
model/dense_1/ReluRelumodel/dense_1/BiasAdd:output:0*
T0*'
_output_shapes
:��������� �
!model/dense/MatMul/ReadVariableOpReadVariableOp*model_dense_matmul_readvariableop_resource*
_output_shapes

:
 *
dtype0�
model/dense/MatMulMatMul+model/dense_features/concat/concat:output:0)model/dense/MatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:��������� �
"model/dense/BiasAdd/ReadVariableOpReadVariableOp+model_dense_biasadd_readvariableop_resource*
_output_shapes
: *
dtype0�
model/dense/BiasAddBiasAddmodel/dense/MatMul:product:0*model/dense/BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:��������� h
model/dense/ReluRelumodel/dense/BiasAdd:output:0*
T0*'
_output_shapes
:��������� �
#model/dense_3/MatMul/ReadVariableOpReadVariableOp,model_dense_3_matmul_readvariableop_resource*
_output_shapes

: @*
dtype0�
model/dense_3/MatMulMatMul model/dense_1/Relu:activations:0+model/dense_3/MatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������@�
$model/dense_3/BiasAdd/ReadVariableOpReadVariableOp-model_dense_3_biasadd_readvariableop_resource*
_output_shapes
:@*
dtype0�
model/dense_3/BiasAddBiasAddmodel/dense_3/MatMul:product:0,model/dense_3/BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������@l
model/dense_3/ReluRelumodel/dense_3/BiasAdd:output:0*
T0*'
_output_shapes
:���������@�
#model/dense_2/MatMul/ReadVariableOpReadVariableOp,model_dense_2_matmul_readvariableop_resource*
_output_shapes

: @*
dtype0�
model/dense_2/MatMulMatMulmodel/dense/Relu:activations:0+model/dense_2/MatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������@�
$model/dense_2/BiasAdd/ReadVariableOpReadVariableOp-model_dense_2_biasadd_readvariableop_resource*
_output_shapes
:@*
dtype0�
model/dense_2/BiasAddBiasAddmodel/dense_2/MatMul:product:0,model/dense_2/BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������@l
model/dense_2/ReluRelumodel/dense_2/BiasAdd:output:0*
T0*'
_output_shapes
:���������@�
#model/dense_4/MatMul/ReadVariableOpReadVariableOp,model_dense_4_matmul_readvariableop_resource*
_output_shapes
:	@�*
dtype0�
model/dense_4/MatMulMatMul model/dense_2/Relu:activations:0+model/dense_4/MatMul/ReadVariableOp:value:0*
T0*(
_output_shapes
:�����������
$model/dense_4/BiasAdd/ReadVariableOpReadVariableOp-model_dense_4_biasadd_readvariableop_resource*
_output_shapes	
:�*
dtype0�
model/dense_4/BiasAddBiasAddmodel/dense_4/MatMul:product:0,model/dense_4/BiasAdd/ReadVariableOp:value:0*
T0*(
_output_shapes
:����������m
model/dense_4/ReluRelumodel/dense_4/BiasAdd:output:0*
T0*(
_output_shapes
:�����������
#model/dense_5/MatMul/ReadVariableOpReadVariableOp,model_dense_5_matmul_readvariableop_resource*
_output_shapes
:	@�*
dtype0�
model/dense_5/MatMulMatMul model/dense_3/Relu:activations:0+model/dense_5/MatMul/ReadVariableOp:value:0*
T0*(
_output_shapes
:�����������
$model/dense_5/BiasAdd/ReadVariableOpReadVariableOp-model_dense_5_biasadd_readvariableop_resource*
_output_shapes	
:�*
dtype0�
model/dense_5/BiasAddBiasAddmodel/dense_5/MatMul:product:0,model/dense_5/BiasAdd/ReadVariableOp:value:0*
T0*(
_output_shapes
:����������m
model/dense_5/ReluRelumodel/dense_5/BiasAdd:output:0*
T0*(
_output_shapes
:����������Z
model/dot/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
value	B :�
model/dot/ExpandDims
ExpandDims model/dense_4/Relu:activations:0!model/dot/ExpandDims/dim:output:0*
T0*,
_output_shapes
:����������\
model/dot/ExpandDims_1/dimConst*
_output_shapes
: *
dtype0*
value	B :�
model/dot/ExpandDims_1
ExpandDims model/dense_5/Relu:activations:0#model/dot/ExpandDims_1/dim:output:0*
T0*,
_output_shapes
:�����������
model/dot/MatMulBatchMatMulV2model/dot/ExpandDims:output:0model/dot/ExpandDims_1:output:0*
T0*+
_output_shapes
:���������f
model/dot/ShapeShapemodel/dot/MatMul:output:0*
T0*
_output_shapes
::���
model/dot/SqueezeSqueezemodel/dot/MatMul:output:0*
T0*'
_output_shapes
:���������*
squeeze_dims
�
#model/dense_6/MatMul/ReadVariableOpReadVariableOp,model_dense_6_matmul_readvariableop_resource*
_output_shapes

:*
dtype0�
model/dense_6/MatMulMatMulmodel/dot/Squeeze:output:0+model/dense_6/MatMul/ReadVariableOp:value:0*
T0*'
_output_shapes
:����������
$model/dense_6/BiasAdd/ReadVariableOpReadVariableOp-model_dense_6_biasadd_readvariableop_resource*
_output_shapes
:*
dtype0�
model/dense_6/BiasAddBiasAddmodel/dense_6/MatMul:product:0,model/dense_6/BiasAdd/ReadVariableOp:value:0*
T0*'
_output_shapes
:���������r
model/dense_6/SigmoidSigmoidmodel/dense_6/BiasAdd:output:0*
T0*'
_output_shapes
:���������h
IdentityIdentitymodel/dense_6/Sigmoid:y:0^NoOp*
T0*'
_output_shapes
:����������
NoOpNoOp#^model/dense/BiasAdd/ReadVariableOp"^model/dense/MatMul/ReadVariableOp%^model/dense_1/BiasAdd/ReadVariableOp$^model/dense_1/MatMul/ReadVariableOp%^model/dense_2/BiasAdd/ReadVariableOp$^model/dense_2/MatMul/ReadVariableOp%^model/dense_3/BiasAdd/ReadVariableOp$^model/dense_3/MatMul/ReadVariableOp%^model/dense_4/BiasAdd/ReadVariableOp$^model/dense_4/MatMul/ReadVariableOp%^model/dense_5/BiasAdd/ReadVariableOp$^model/dense_5/MatMul/ReadVariableOp%^model/dense_6/BiasAdd/ReadVariableOp$^model/dense_6/MatMul/ReadVariableOpj^model/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupj^model/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime*Q
_input_shapes@
>:���������:���������: : : : : : : : : : : : : : : : 2H
"model/dense/BiasAdd/ReadVariableOp"model/dense/BiasAdd/ReadVariableOp2F
!model/dense/MatMul/ReadVariableOp!model/dense/MatMul/ReadVariableOp2L
$model/dense_1/BiasAdd/ReadVariableOp$model/dense_1/BiasAdd/ReadVariableOp2J
#model/dense_1/MatMul/ReadVariableOp#model/dense_1/MatMul/ReadVariableOp2L
$model/dense_2/BiasAdd/ReadVariableOp$model/dense_2/BiasAdd/ReadVariableOp2J
#model/dense_2/MatMul/ReadVariableOp#model/dense_2/MatMul/ReadVariableOp2L
$model/dense_3/BiasAdd/ReadVariableOp$model/dense_3/BiasAdd/ReadVariableOp2J
#model/dense_3/MatMul/ReadVariableOp#model/dense_3/MatMul/ReadVariableOp2L
$model/dense_4/BiasAdd/ReadVariableOp$model/dense_4/BiasAdd/ReadVariableOp2J
#model/dense_4/MatMul/ReadVariableOp#model/dense_4/MatMul/ReadVariableOp2L
$model/dense_5/BiasAdd/ReadVariableOp$model/dense_5/BiasAdd/ReadVariableOp2J
#model/dense_5/MatMul/ReadVariableOp#model/dense_5/MatMul/ReadVariableOp2L
$model/dense_6/BiasAdd/ReadVariableOp$model/dense_6/BiasAdd/ReadVariableOp2J
#model/dense_6/MatMul/ReadVariableOp#model/dense_6/MatMul/ReadVariableOp2�
imodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookupimodel/dense_features/movieId_embedding/movieId_embedding_weights/embedding_lookup_sparse/embedding_lookup2�
imodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookupimodel/dense_features_1/userId_embedding/userId_embedding_weights/embedding_lookup_sparse/embedding_lookup:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:(
$
"
_user_specified_name
resource:(	$
"
_user_specified_name
resource:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:($
"
_user_specified_name
resource:&"
 
_user_specified_name402441:&"
 
_user_specified_name402362:KG
#
_output_shapes
:���������
 
_user_specified_nameuserId:L H
#
_output_shapes
:���������
!
_user_specified_name	movieId
�
�
(__inference_dense_5_layer_call_fn_403750

inputs
unknown:	@�
	unknown_0:	�
identity��StatefulPartitionedCall�
StatefulPartitionedCallStatefulPartitionedCallinputsunknown	unknown_0*
Tin
2*
Tout
2*
_collective_manager_ids
 *(
_output_shapes
:����������*$
_read_only_resource_inputs
*-
config_proto

CPU

GPU 2J 8� *L
fGRE
C__inference_dense_5_layer_call_and_return_conditional_losses_402804p
IdentityIdentity StatefulPartitionedCall:output:0^NoOp*
T0*(
_output_shapes
:����������<
NoOpNoOp^StatefulPartitionedCall*
_output_shapes
 "
identityIdentity:output:0*(
_construction_contextkEagerRuntime**
_input_shapes
:���������@: : 22
StatefulPartitionedCallStatefulPartitionedCall:&"
 
_user_specified_name403746:&"
 
_user_specified_name403744:O K
'
_output_shapes
:���������@
 
_user_specified_nameinputs"�L
saver_filename:0StatefulPartitionedCall_1:0StatefulPartitionedCall_28"
saved_model_main_op

NoOp*>
__saved_model_init_op%#
__saved_model_init_op

NoOp*�
serving_default�
7
movieId,
serving_default_movieId:0���������
5
userId+
serving_default_userId:0���������;
dense_60
StatefulPartitionedCall:0���������tensorflow/serving/predict:��
�
layer-0
layer-1
layer_with_weights-0
layer-2
layer_with_weights-1
layer-3
layer_with_weights-2
layer-4
layer_with_weights-3
layer-5
layer_with_weights-4
layer-6
layer_with_weights-5
layer-7
	layer_with_weights-6
	layer-8

layer_with_weights-7

layer-9
layer-10
layer_with_weights-8
layer-11
	variables
trainable_variables
regularization_losses
	keras_api
__call__
*&call_and_return_all_conditional_losses
_default_save_signature
	optimizer

signatures"
_tf_keras_network
"
_tf_keras_input_layer
"
_tf_keras_input_layer
�
	variables
trainable_variables
regularization_losses
	keras_api
__call__
*&call_and_return_all_conditional_losses
_feature_columns

_resources
'#movieId_embedding/embedding_weights"
_tf_keras_layer
�
	variables
 trainable_variables
!regularization_losses
"	keras_api
#__call__
*$&call_and_return_all_conditional_losses
%_feature_columns
&
_resources
&'"userId_embedding/embedding_weights"
_tf_keras_layer
�
(	variables
)trainable_variables
*regularization_losses
+	keras_api
,__call__
*-&call_and_return_all_conditional_losses

.kernel
/bias"
_tf_keras_layer
�
0	variables
1trainable_variables
2regularization_losses
3	keras_api
4__call__
*5&call_and_return_all_conditional_losses

6kernel
7bias"
_tf_keras_layer
�
8	variables
9trainable_variables
:regularization_losses
;	keras_api
<__call__
*=&call_and_return_all_conditional_losses

>kernel
?bias"
_tf_keras_layer
�
@	variables
Atrainable_variables
Bregularization_losses
C	keras_api
D__call__
*E&call_and_return_all_conditional_losses

Fkernel
Gbias"
_tf_keras_layer
�
H	variables
Itrainable_variables
Jregularization_losses
K	keras_api
L__call__
*M&call_and_return_all_conditional_losses

Nkernel
Obias"
_tf_keras_layer
�
P	variables
Qtrainable_variables
Rregularization_losses
S	keras_api
T__call__
*U&call_and_return_all_conditional_losses

Vkernel
Wbias"
_tf_keras_layer
�
X	variables
Ytrainable_variables
Zregularization_losses
[	keras_api
\__call__
*]&call_and_return_all_conditional_losses"
_tf_keras_layer
�
^	variables
_trainable_variables
`regularization_losses
a	keras_api
b__call__
*c&call_and_return_all_conditional_losses

dkernel
ebias"
_tf_keras_layer
�
0
'1
.2
/3
64
75
>6
?7
F8
G9
N10
O11
V12
W13
d14
e15"
trackable_list_wrapper
�
0
'1
.2
/3
64
75
>6
?7
F8
G9
N10
O11
V12
W13
d14
e15"
trackable_list_wrapper
 "
trackable_list_wrapper
�
fnon_trainable_variables

glayers
hmetrics
ilayer_regularization_losses
jlayer_metrics
	variables
trainable_variables
regularization_losses
__call__
_default_save_signature
*&call_and_return_all_conditional_losses
&"call_and_return_conditional_losses"
_generic_user_object
�
ktrace_0
ltrace_12�
&__inference_model_layer_call_fn_403093
&__inference_model_layer_call_fn_403131�
���
FullArgSpec)
args!�
jinputs

jtraining
jmask
varargs
 
varkw
 
defaults�
p 

 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 zktrace_0zltrace_1
�
mtrace_0
ntrace_12�
A__inference_model_layer_call_and_return_conditional_losses_402840
A__inference_model_layer_call_and_return_conditional_losses_403055�
���
FullArgSpec)
args!�
jinputs

jtraining
jmask
varargs
 
varkw
 
defaults�
p 

 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 zmtrace_0zntrace_1
�B�
!__inference__wrapped_model_402536movieIduserId"�
���
FullArgSpec
args� 
varargsjargs
varkwjkwargs
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�
o
_variables
p_iterations
q_learning_rate
r_index_dict
s
_momentums
t_velocities
u_update_step_xla"
experimentalOptimizer
,
vserving_default"
signature_map
'
0"
trackable_list_wrapper
'
0"
trackable_list_wrapper
 "
trackable_list_wrapper
�
wnon_trainable_variables

xlayers
ymetrics
zlayer_regularization_losses
{layer_metrics
	variables
trainable_variables
regularization_losses
__call__
*&call_and_return_all_conditional_losses
&"call_and_return_conditional_losses"
_generic_user_object
�
|trace_0
}trace_12�
/__inference_dense_features_layer_call_fn_403281
/__inference_dense_features_layer_call_fn_403289�
���
FullArgSpec=
args5�2

jfeatures
jcols_to_output_tensors

jtraining
varargs
 
varkw
 
defaults�

 
p 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z|trace_0z}trace_1
�
~trace_0
trace_12�
J__inference_dense_features_layer_call_and_return_conditional_losses_403373
J__inference_dense_features_layer_call_and_return_conditional_losses_403457�
���
FullArgSpec=
args5�2

jfeatures
jcols_to_output_tensors

jtraining
varargs
 
varkw
 
defaults�

 
p 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z~trace_0ztrace_1
 "
trackable_list_wrapper
"
_generic_user_object
F:D
��
22dense_features/movieId_embedding/embedding_weights
'
'0"
trackable_list_wrapper
'
'0"
trackable_list_wrapper
 "
trackable_list_wrapper
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
	variables
 trainable_variables
!regularization_losses
#__call__
*$&call_and_return_all_conditional_losses
&$"call_and_return_conditional_losses"
_generic_user_object
�
�trace_0
�trace_12�
1__inference_dense_features_1_layer_call_fn_403465
1__inference_dense_features_1_layer_call_fn_403473�
���
FullArgSpec=
args5�2

jfeatures
jcols_to_output_tensors

jtraining
varargs
 
varkw
 
defaults�

 
p 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0z�trace_1
�
�trace_0
�trace_12�
L__inference_dense_features_1_layer_call_and_return_conditional_losses_403557
L__inference_dense_features_1_layer_call_and_return_conditional_losses_403641�
���
FullArgSpec=
args5�2

jfeatures
jcols_to_output_tensors

jtraining
varargs
 
varkw
 
defaults�

 
p 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0z�trace_1
 "
trackable_list_wrapper
"
_generic_user_object
G:E
��
23dense_features_1/userId_embedding/embedding_weights
.
.0
/1"
trackable_list_wrapper
.
.0
/1"
trackable_list_wrapper
 "
trackable_list_wrapper
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
(	variables
)trainable_variables
*regularization_losses
,__call__
*-&call_and_return_all_conditional_losses
&-"call_and_return_conditional_losses"
_generic_user_object
�
�trace_02�
&__inference_dense_layer_call_fn_403650�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
�
�trace_02�
A__inference_dense_layer_call_and_return_conditional_losses_403661�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
:
 2dense/kernel
: 2
dense/bias
.
60
71"
trackable_list_wrapper
.
60
71"
trackable_list_wrapper
 "
trackable_list_wrapper
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
0	variables
1trainable_variables
2regularization_losses
4__call__
*5&call_and_return_all_conditional_losses
&5"call_and_return_conditional_losses"
_generic_user_object
�
�trace_02�
(__inference_dense_1_layer_call_fn_403670�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
�
�trace_02�
C__inference_dense_1_layer_call_and_return_conditional_losses_403681�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
 :
 2dense_1/kernel
: 2dense_1/bias
.
>0
?1"
trackable_list_wrapper
.
>0
?1"
trackable_list_wrapper
 "
trackable_list_wrapper
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
8	variables
9trainable_variables
:regularization_losses
<__call__
*=&call_and_return_all_conditional_losses
&="call_and_return_conditional_losses"
_generic_user_object
�
�trace_02�
(__inference_dense_2_layer_call_fn_403690�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
�
�trace_02�
C__inference_dense_2_layer_call_and_return_conditional_losses_403701�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
 : @2dense_2/kernel
:@2dense_2/bias
.
F0
G1"
trackable_list_wrapper
.
F0
G1"
trackable_list_wrapper
 "
trackable_list_wrapper
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
@	variables
Atrainable_variables
Bregularization_losses
D__call__
*E&call_and_return_all_conditional_losses
&E"call_and_return_conditional_losses"
_generic_user_object
�
�trace_02�
(__inference_dense_3_layer_call_fn_403710�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
�
�trace_02�
C__inference_dense_3_layer_call_and_return_conditional_losses_403721�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
 : @2dense_3/kernel
:@2dense_3/bias
.
N0
O1"
trackable_list_wrapper
.
N0
O1"
trackable_list_wrapper
 "
trackable_list_wrapper
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
H	variables
Itrainable_variables
Jregularization_losses
L__call__
*M&call_and_return_all_conditional_losses
&M"call_and_return_conditional_losses"
_generic_user_object
�
�trace_02�
(__inference_dense_4_layer_call_fn_403730�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
�
�trace_02�
C__inference_dense_4_layer_call_and_return_conditional_losses_403741�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
!:	@�2dense_4/kernel
:�2dense_4/bias
.
V0
W1"
trackable_list_wrapper
.
V0
W1"
trackable_list_wrapper
 "
trackable_list_wrapper
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
P	variables
Qtrainable_variables
Rregularization_losses
T__call__
*U&call_and_return_all_conditional_losses
&U"call_and_return_conditional_losses"
_generic_user_object
�
�trace_02�
(__inference_dense_5_layer_call_fn_403750�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
�
�trace_02�
C__inference_dense_5_layer_call_and_return_conditional_losses_403761�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
!:	@�2dense_5/kernel
:�2dense_5/bias
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
X	variables
Ytrainable_variables
Zregularization_losses
\__call__
*]&call_and_return_all_conditional_losses
&]"call_and_return_conditional_losses"
_generic_user_object
�
�trace_02�
$__inference_dot_layer_call_fn_403767�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
�
�trace_02�
?__inference_dot_layer_call_and_return_conditional_losses_403779�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
.
d0
e1"
trackable_list_wrapper
.
d0
e1"
trackable_list_wrapper
 "
trackable_list_wrapper
�
�non_trainable_variables
�layers
�metrics
 �layer_regularization_losses
�layer_metrics
^	variables
_trainable_variables
`regularization_losses
b__call__
*c&call_and_return_all_conditional_losses
&c"call_and_return_conditional_losses"
_generic_user_object
�
�trace_02�
(__inference_dense_6_layer_call_fn_403788�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
�
�trace_02�
C__inference_dense_6_layer_call_and_return_conditional_losses_403799�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 z�trace_0
 :2dense_6/kernel
:2dense_6/bias
 "
trackable_list_wrapper
v
0
1
2
3
4
5
6
7
	8

9
10
11"
trackable_list_wrapper
@
�0
�1
�2
�3"
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_dict_wrapper
�B�
&__inference_model_layer_call_fn_403093movieIduserId"�
���
FullArgSpec)
args!�
jinputs

jtraining
jmask
varargs
 
varkw
 
defaults�
p 

 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
&__inference_model_layer_call_fn_403131movieIduserId"�
���
FullArgSpec)
args!�
jinputs

jtraining
jmask
varargs
 
varkw
 
defaults�
p 

 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
A__inference_model_layer_call_and_return_conditional_losses_402840movieIduserId"�
���
FullArgSpec)
args!�
jinputs

jtraining
jmask
varargs
 
varkw
 
defaults�
p 

 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
A__inference_model_layer_call_and_return_conditional_losses_403055movieIduserId"�
���
FullArgSpec)
args!�
jinputs

jtraining
jmask
varargs
 
varkw
 
defaults�
p 

 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�
p0
�1
�2
�3
�4
�5
�6
�7
�8
�9
�10
�11
�12
�13
�14
�15
�16
�17
�18
�19
�20
�21
�22
�23
�24
�25
�26
�27
�28
�29
�30
�31
�32"
trackable_list_wrapper
:	 2	iteration
: 2learning_rate
 "
trackable_dict_wrapper
�
�0
�1
�2
�3
�4
�5
�6
�7
�8
�9
�10
�11
�12
�13
�14
�15"
trackable_list_wrapper
�
�0
�1
�2
�3
�4
�5
�6
�7
�8
�9
�10
�11
�12
�13
�14
�15"
trackable_list_wrapper
�2��
���
FullArgSpec*
args"�

jgradient

jvariable
jkey
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 0
�B�
$__inference_signature_wrapper_403273movieIduserId"�
���
FullArgSpec
args� 
varargs
 
varkwjkwargs
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_dict_wrapper
�B�
/__inference_dense_features_layer_call_fn_403281features_movieidfeatures_userid"�
���
FullArgSpec=
args5�2

jfeatures
jcols_to_output_tensors

jtraining
varargs
 
varkw
 
defaults�

 
p 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
/__inference_dense_features_layer_call_fn_403289features_movieidfeatures_userid"�
���
FullArgSpec=
args5�2

jfeatures
jcols_to_output_tensors

jtraining
varargs
 
varkw
 
defaults�

 
p 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
J__inference_dense_features_layer_call_and_return_conditional_losses_403373features_movieidfeatures_userid"�
���
FullArgSpec=
args5�2

jfeatures
jcols_to_output_tensors

jtraining
varargs
 
varkw
 
defaults�

 
p 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
J__inference_dense_features_layer_call_and_return_conditional_losses_403457features_movieidfeatures_userid"�
���
FullArgSpec=
args5�2

jfeatures
jcols_to_output_tensors

jtraining
varargs
 
varkw
 
defaults�

 
p 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_dict_wrapper
�B�
1__inference_dense_features_1_layer_call_fn_403465features_movieidfeatures_userid"�
���
FullArgSpec=
args5�2

jfeatures
jcols_to_output_tensors

jtraining
varargs
 
varkw
 
defaults�

 
p 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
1__inference_dense_features_1_layer_call_fn_403473features_movieidfeatures_userid"�
���
FullArgSpec=
args5�2

jfeatures
jcols_to_output_tensors

jtraining
varargs
 
varkw
 
defaults�

 
p 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
L__inference_dense_features_1_layer_call_and_return_conditional_losses_403557features_movieidfeatures_userid"�
���
FullArgSpec=
args5�2

jfeatures
jcols_to_output_tensors

jtraining
varargs
 
varkw
 
defaults�

 
p 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
L__inference_dense_features_1_layer_call_and_return_conditional_losses_403641features_movieidfeatures_userid"�
���
FullArgSpec=
args5�2

jfeatures
jcols_to_output_tensors

jtraining
varargs
 
varkw
 
defaults�

 
p 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_dict_wrapper
�B�
&__inference_dense_layer_call_fn_403650inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
A__inference_dense_layer_call_and_return_conditional_losses_403661inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_dict_wrapper
�B�
(__inference_dense_1_layer_call_fn_403670inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
C__inference_dense_1_layer_call_and_return_conditional_losses_403681inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_dict_wrapper
�B�
(__inference_dense_2_layer_call_fn_403690inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
C__inference_dense_2_layer_call_and_return_conditional_losses_403701inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_dict_wrapper
�B�
(__inference_dense_3_layer_call_fn_403710inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
C__inference_dense_3_layer_call_and_return_conditional_losses_403721inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_dict_wrapper
�B�
(__inference_dense_4_layer_call_fn_403730inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
C__inference_dense_4_layer_call_and_return_conditional_losses_403741inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_dict_wrapper
�B�
(__inference_dense_5_layer_call_fn_403750inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
C__inference_dense_5_layer_call_and_return_conditional_losses_403761inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_dict_wrapper
�B�
$__inference_dot_layer_call_fn_403767inputs_0inputs_1"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
?__inference_dot_layer_call_and_return_conditional_losses_403779inputs_0inputs_1"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_list_wrapper
 "
trackable_dict_wrapper
�B�
(__inference_dense_6_layer_call_fn_403788inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
�B�
C__inference_dense_6_layer_call_and_return_conditional_losses_403799inputs"�
���
FullArgSpec
args�

jinputs
varargs
 
varkw
 
defaults
 

kwonlyargs� 
kwonlydefaults
 
annotations� *
 
R
�	variables
�	keras_api

�total

�count"
_tf_keras_metric
c
�	variables
�	keras_api

�total

�count
�
_fn_kwargs"
_tf_keras_metric
�
�	variables
�	keras_api
�true_positives
�true_negatives
�false_positives
�false_negatives"
_tf_keras_metric
�
�	variables
�	keras_api
�true_positives
�true_negatives
�false_positives
�false_negatives"
_tf_keras_metric
K:I
��
29Adam/m/dense_features/movieId_embedding/embedding_weights
K:I
��
29Adam/v/dense_features/movieId_embedding/embedding_weights
L:J
��
2:Adam/m/dense_features_1/userId_embedding/embedding_weights
L:J
��
2:Adam/v/dense_features_1/userId_embedding/embedding_weights
#:!
 2Adam/m/dense/kernel
#:!
 2Adam/v/dense/kernel
: 2Adam/m/dense/bias
: 2Adam/v/dense/bias
%:#
 2Adam/m/dense_1/kernel
%:#
 2Adam/v/dense_1/kernel
: 2Adam/m/dense_1/bias
: 2Adam/v/dense_1/bias
%:# @2Adam/m/dense_2/kernel
%:# @2Adam/v/dense_2/kernel
:@2Adam/m/dense_2/bias
:@2Adam/v/dense_2/bias
%:# @2Adam/m/dense_3/kernel
%:# @2Adam/v/dense_3/kernel
:@2Adam/m/dense_3/bias
:@2Adam/v/dense_3/bias
&:$	@�2Adam/m/dense_4/kernel
&:$	@�2Adam/v/dense_4/kernel
 :�2Adam/m/dense_4/bias
 :�2Adam/v/dense_4/bias
&:$	@�2Adam/m/dense_5/kernel
&:$	@�2Adam/v/dense_5/kernel
 :�2Adam/m/dense_5/bias
 :�2Adam/v/dense_5/bias
%:#2Adam/m/dense_6/kernel
%:#2Adam/v/dense_6/kernel
:2Adam/m/dense_6/bias
:2Adam/v/dense_6/bias
0
�0
�1"
trackable_list_wrapper
.
�	variables"
_generic_user_object
:  (2total
:  (2count
0
�0
�1"
trackable_list_wrapper
.
�	variables"
_generic_user_object
:  (2total
:  (2count
 "
trackable_dict_wrapper
@
�0
�1
�2
�3"
trackable_list_wrapper
.
�	variables"
_generic_user_object
:� (2true_positives
:� (2true_negatives
 :� (2false_positives
 :� (2false_negatives
@
�0
�1
�2
�3"
trackable_list_wrapper
.
�	variables"
_generic_user_object
:� (2true_positives
:� (2true_negatives
 :� (2false_positives
 :� (2false_negatives�
!__inference__wrapped_model_402536�'67./FG>?NOVWded�a
Z�W
U�R
(
movieId�
movieId���������
&
userId�
userId���������
� "1�.
,
dense_6!�
dense_6����������
C__inference_dense_1_layer_call_and_return_conditional_losses_403681c67/�,
%�"
 �
inputs���������

� ",�)
"�
tensor_0��������� 
� �
(__inference_dense_1_layer_call_fn_403670X67/�,
%�"
 �
inputs���������

� "!�
unknown��������� �
C__inference_dense_2_layer_call_and_return_conditional_losses_403701c>?/�,
%�"
 �
inputs��������� 
� ",�)
"�
tensor_0���������@
� �
(__inference_dense_2_layer_call_fn_403690X>?/�,
%�"
 �
inputs��������� 
� "!�
unknown���������@�
C__inference_dense_3_layer_call_and_return_conditional_losses_403721cFG/�,
%�"
 �
inputs��������� 
� ",�)
"�
tensor_0���������@
� �
(__inference_dense_3_layer_call_fn_403710XFG/�,
%�"
 �
inputs��������� 
� "!�
unknown���������@�
C__inference_dense_4_layer_call_and_return_conditional_losses_403741dNO/�,
%�"
 �
inputs���������@
� "-�*
#� 
tensor_0����������
� �
(__inference_dense_4_layer_call_fn_403730YNO/�,
%�"
 �
inputs���������@
� ""�
unknown�����������
C__inference_dense_5_layer_call_and_return_conditional_losses_403761dVW/�,
%�"
 �
inputs���������@
� "-�*
#� 
tensor_0����������
� �
(__inference_dense_5_layer_call_fn_403750YVW/�,
%�"
 �
inputs���������@
� ""�
unknown�����������
C__inference_dense_6_layer_call_and_return_conditional_losses_403799cde/�,
%�"
 �
inputs���������
� ",�)
"�
tensor_0���������
� �
(__inference_dense_6_layer_call_fn_403788Xde/�,
%�"
 �
inputs���������
� "!�
unknown����������
L__inference_dense_features_1_layer_call_and_return_conditional_losses_403557�'~�{
t�q
g�d
1
movieId&�#
features_movieid���������
/
userId%�"
features_userid���������

 
p
� ",�)
"�
tensor_0���������

� �
L__inference_dense_features_1_layer_call_and_return_conditional_losses_403641�'~�{
t�q
g�d
1
movieId&�#
features_movieid���������
/
userId%�"
features_userid���������

 
p 
� ",�)
"�
tensor_0���������

� �
1__inference_dense_features_1_layer_call_fn_403465�'~�{
t�q
g�d
1
movieId&�#
features_movieid���������
/
userId%�"
features_userid���������

 
p
� "!�
unknown���������
�
1__inference_dense_features_1_layer_call_fn_403473�'~�{
t�q
g�d
1
movieId&�#
features_movieid���������
/
userId%�"
features_userid���������

 
p 
� "!�
unknown���������
�
J__inference_dense_features_layer_call_and_return_conditional_losses_403373�~�{
t�q
g�d
1
movieId&�#
features_movieid���������
/
userId%�"
features_userid���������

 
p
� ",�)
"�
tensor_0���������

� �
J__inference_dense_features_layer_call_and_return_conditional_losses_403457�~�{
t�q
g�d
1
movieId&�#
features_movieid���������
/
userId%�"
features_userid���������

 
p 
� ",�)
"�
tensor_0���������

� �
/__inference_dense_features_layer_call_fn_403281�~�{
t�q
g�d
1
movieId&�#
features_movieid���������
/
userId%�"
features_userid���������

 
p
� "!�
unknown���������
�
/__inference_dense_features_layer_call_fn_403289�~�{
t�q
g�d
1
movieId&�#
features_movieid���������
/
userId%�"
features_userid���������

 
p 
� "!�
unknown���������
�
A__inference_dense_layer_call_and_return_conditional_losses_403661c.//�,
%�"
 �
inputs���������

� ",�)
"�
tensor_0��������� 
� �
&__inference_dense_layer_call_fn_403650X.//�,
%�"
 �
inputs���������

� "!�
unknown��������� �
?__inference_dot_layer_call_and_return_conditional_losses_403779�\�Y
R�O
M�J
#� 
inputs_0����������
#� 
inputs_1����������
� ",�)
"�
tensor_0���������
� �
$__inference_dot_layer_call_fn_403767�\�Y
R�O
M�J
#� 
inputs_0����������
#� 
inputs_1����������
� "!�
unknown����������
A__inference_model_layer_call_and_return_conditional_losses_402840�'67./FG>?NOVWdel�i
b�_
U�R
(
movieId�
movieId���������
&
userId�
userId���������
p

 
� ",�)
"�
tensor_0���������
� �
A__inference_model_layer_call_and_return_conditional_losses_403055�'67./FG>?NOVWdel�i
b�_
U�R
(
movieId�
movieId���������
&
userId�
userId���������
p 

 
� ",�)
"�
tensor_0���������
� �
&__inference_model_layer_call_fn_403093�'67./FG>?NOVWdel�i
b�_
U�R
(
movieId�
movieId���������
&
userId�
userId���������
p

 
� "!�
unknown����������
&__inference_model_layer_call_fn_403131�'67./FG>?NOVWdel�i
b�_
U�R
(
movieId�
movieId���������
&
userId�
userId���������
p 

 
� "!�
unknown����������
$__inference_signature_wrapper_403273�'67./FG>?NOVWde_�\
� 
U�R
(
movieId�
movieid���������
&
userId�
userid���������"1�.
,
dense_6!�
dense_6���������