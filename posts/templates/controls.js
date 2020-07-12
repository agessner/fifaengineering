const createImageControls = (image, topField, leftField, widthField, heightField) => {
    topField.on('input', event => {
        updateProperty('top', event)
    })

    leftField.on('input', event => {
        updateProperty('left', event)
    })

    widthField.on('input', event => {
        updateProperty('width', event)
    })

    heightField.on('input', event => {
        updateProperty('height', event)
    })

    const updateProperty = (property, event) => {
        image.css(property, event.target.value + 'px')
    }
}