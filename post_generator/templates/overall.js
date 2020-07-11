const updateOverallColor = (element, value) => {
    element.removeClass('lower-than-61')
    element.removeClass('lower-than-71')
    element.removeClass('lower-than-81')
    element.removeClass('lower-than-99')
    element.addClass(getRatingClass(value))
}

const getRatingClass = overall => {
    if (parseInt(overall) < 61) {
        return 'lower-than-61'
    }

    if (parseInt(overall) < 71) {
        return 'lower-than-71'
    }

    if (parseInt(overall) < 81) {
        return 'lower-than-81'
    }
    return 'lower-than-99'
}