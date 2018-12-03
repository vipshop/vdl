package main

import "testing"

//sliceA-sliceB
func TestSliceDiff(t *testing.T) {
	snap1 := &SnapFile{
		FileName: "1.log",
		FileType: SegmentFileType,
		IsLast:   false,
	}
	snap2 := &SnapFile{
		FileName: "2.log",
		FileType: SegmentFileType,
		IsLast:   false,
	}
	snap3 := &SnapFile{
		FileName: "3.log",
		FileType: SegmentFileType,
		IsLast:   false,
	}
	sliceA := []*SnapFile{snap1, snap2, snap3}
	sliceB := []*SnapFile{snap1, snap2, snap3}
	diffs := sliceDiff(sliceA, sliceB)
	if len(diffs) != 0 {
		t.Fatalf("want len(diffs)=0,but is %d", len(diffs))
	}
	sliceC := make([]*SnapFile, 0, 1)
	diffs2 := sliceDiff(sliceA, sliceC)
	if len(diffs2) != 3 {
		t.Fatalf("want len(diffs)=3,but is %d", len(diffs2))
	}
}

//sliceA与sliceB的交集
func TestSliceIntersect(t *testing.T) {
	snap1 := &SnapFile{
		FileName: "1.log",
		FileType: SegmentFileType,
		IsLast:   false,
	}
	snap2 := &SnapFile{
		FileName: "2.log",
		FileType: SegmentFileType,
		IsLast:   false,
	}
	snap3 := &SnapFile{
		FileName: "3.log",
		FileType: SegmentFileType,
		IsLast:   false,
	}
	sliceA := []*SnapFile{snap1, snap2, snap3}
	sliceB := []*SnapFile{snap1, snap2, snap3}
	intersect := sliceIntersect(sliceA, sliceB)
	if len(intersect) != 3 {
		t.Fatalf("want len(diffs)=3,but is %d", len(intersect))
	}
	sliceC := make([]*SnapFile, 0, 1)
	intersect2 := sliceIntersect(sliceA, sliceC)
	if len(intersect2) != 0 {
		t.Fatalf("want len(diffs)=0,but is %d", len(intersect2))
	}
}
