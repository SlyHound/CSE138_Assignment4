package utility

// removes duplicates in every view for GET responses //
func DeleteDuplicates(view []string) []string {
	var newView []string
	exists := make(map[string]string)

	for index, socketAddr := range view {
		if socketAddr != exists[view[index]] {
			newView = append(newView, socketAddr)
			exists[view[index]] = socketAddr
		}
	}
	return newView
}
